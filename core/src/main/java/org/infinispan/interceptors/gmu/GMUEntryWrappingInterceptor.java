package org.infinispan.interceptors.gmu;

import org.infinispan.CacheException;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.CommitQueue;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;

import static org.infinispan.transaction.gmu.GMUHelper.*;

/**
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUEntryWrappingInterceptor extends EntryWrappingInterceptor implements CommitQueue.CommitInstance {

   private static final Log log = LogFactory.getLog(GMUEntryWrappingInterceptor.class);

   private CommitQueue commitQueue;
   private GMUVersionGenerator versionGenerator;

   @Inject
   public void inject(CommitQueue commitQueue, DataContainer dataContainer, CommitLog commitLog, VersionGenerator versionGenerator) {
      this.commitQueue = commitQueue;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   @Override

   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      GMUPrepareCommand spc = convert(command, GMUPrepareCommand.class);

      if (ctx.isOriginLocal()) {
         spc.setVersion(ctx.getTransactionVersion());
         spc.setReadSet(ctx.getReadSet());
      } else {
         ctx.setTransactionVersion(spc.getPrepareVersion());
      }

      wrapEntriesForPrepare(ctx, command);
      EntryVersion localPrepareVersion = performValidation(ctx, spc);

      Object retVal = invokeNextInterceptor(ctx, command);

      if (ctx.isOriginLocal()) {
         EntryVersion commitVersion = calculateCommitVersion(localPrepareVersion, convert(retVal, EntryVersion.class),
                                                             versionGenerator, cll.getOwners(command.getAffectedKeys()));
         ctx.setTransactionVersion(commitVersion);
      } else {
         ctx.setTransactionVersion(localPrepareVersion);
      }

      return retVal;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      GMUCommitCommand gmuCommitCommand = convert(command, GMUCommitCommand.class);

      if (ctx.isOriginLocal()) {
         gmuCommitCommand.setCommitVersion(ctx.getTransactionVersion());
      }

      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitQueue.commitTransaction(gmuCommitCommand.getGlobalTransaction(),
                                       gmuCommitCommand.getCommitVersion(),
                                       configuration.isSyncCommitPhase());
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitQueue.rollbackTransaction(command.getGlobalTransaction());
      }
   }

   /*
    * NOTE: these are the only commands that passes values to the application and these keys needs to be validated
    * and added to the transaction read set.
    */

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitGetKeyValueCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal =  super.visitPutKeyValueCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal =  super.visitRemoveCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal =  super.visitReplaceCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public void commitContextEntries(TxInvocationContext ctx, GMUEntryVersion commitVersion) {
      commitContextEntries(ctx);
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      if (ctx.isInTxScope()) {
         cll.commitEntry(entry, ((TxInvocationContext)ctx).getTransactionVersion(), skipOwnershipCheck);
      } else {
         cll.commitEntry(entry, entry.getVersion(), skipOwnershipCheck);
      }
   }

   /**
    * validates the read set and returns the prepare version from the commit queue
    *
    * @param ctx     the context
    * @param command the prepare command
    * @return        the prepare version for the transaction
    * @throws InterruptedException  if interrupted
    */
   private EntryVersion performValidation(TxInvocationContext ctx, GMUPrepareCommand command) throws InterruptedException {
      cll.performReadSetValidation(ctx, command);

      boolean hasToUpdateLocalKeys = false;

      for (Object key : command.getAffectedKeys()) {
         if (cll.localNodeIsOwner(key)) {
            hasToUpdateLocalKeys = true;
            break;
         }
      }

      EntryVersion prepareVersion = hasToUpdateLocalKeys ?
            commitQueue.prepareTransaction(command.getGlobalTransaction(), (TxInvocationContext) ctx.clone()) :
            commitQueue.prepareReadOnlyTransaction((TxInvocationContext) ctx.clone());

      if(log.isDebugEnabled()) {
         log.debugf("Transaction %s can commit on this node and it is a %s transaction. Prepare Version is %s",
                    command.getGlobalTransaction().prettyPrint(), hasToUpdateLocalKeys ? "Read-Write" : "Read-Only",
                    prepareVersion);
      }
      return prepareVersion;
   }

   private void updateTransactionVersion(InvocationContext context) {
      if (!context.isInTxScope() && !context.isOriginLocal()) {
         return;
      }

      TxInvocationContext txInvocationContext = (TxInvocationContext) context;
      List<EntryVersion> entryVersionList = new LinkedList<EntryVersion>();
      entryVersionList.add(txInvocationContext.getTransactionVersion());

      for (InternalGMUCacheEntry internalGMUCacheEntry : txInvocationContext.getKeysReadInCommand().values()) {
         if (txInvocationContext.hasModifications() && !internalGMUCacheEntry.isMostRecent()) {
            throw new CacheException("Read-Write transaction read an old value and should rollback");
         }

         if (internalGMUCacheEntry.getMaximumTransactionVersion() != null) {
            entryVersionList.add(internalGMUCacheEntry.getMaximumTransactionVersion());
         }
         txInvocationContext.getCacheTransaction().addReadKey(internalGMUCacheEntry.getKey());
         if (cll.localNodeIsOwner(internalGMUCacheEntry.getKey())) {
            txInvocationContext.setAlreadyReadOnThisNode(true);
         }
      }

      if (entryVersionList.size() > 1) {
         txInvocationContext.setTransactionVersion(versionGenerator.mergeAndMax(entryVersionList));
      }
   }

}
