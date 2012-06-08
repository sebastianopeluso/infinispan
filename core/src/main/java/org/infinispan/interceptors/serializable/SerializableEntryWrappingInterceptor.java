package org.infinispan.interceptors.serializable;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.SerializableCommitCommand;
import org.infinispan.commands.tx.SerializablePrepareCommand;
import org.infinispan.container.MultiVersionDataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.exception.ValidationException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SerializableEntryWrappingInterceptor extends EntryWrappingInterceptor implements CommitQueue.CommitInstance {

   private static final Log log = LogFactory.getLog(SerializableEntryWrappingInterceptor.class);

   private CommitQueue commitQueue;
   protected MultiVersionDataContainer dataContainer;

   @Inject
   public void inject(CommitQueue commitQueue, MultiVersionDataContainer dataContainer) {
      this.commitQueue = commitQueue;
      this.dataContainer = dataContainer;
   }

   @Override

   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!(command instanceof SerializablePrepareCommand)) {
         throw new IllegalStateException("Expected Serializable Prepare Command but it is " + command.getClass().getSimpleName());
      }

      wrapEntriesForPrepare(ctx, command);
      validateReadSet(ctx, (SerializablePrepareCommand) command);
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!(command instanceof SerializableCommitCommand)) {
         throw new IllegalStateException("Expected Serializable Commit Command but it is " + command.getClass().getSimpleName());
      }

      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitQueue.updateAndWait(command.getGlobalTransaction(), ((SerializableCommitCommand)command).getCommitVersion());
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitQueue.remove(command.getGlobalTransaction());
      }
   }

   @Override
   public void commit(InvocationContext ctx, VersionVC commitVersion) {
      final boolean trace = log.isTraceEnabled();
      boolean skipOwnershipCheck = ctx.hasFlag(Flag.SKIP_OWNERSHIP_CHECK);

      if (ctx instanceof SingleKeyNonTxInvocationContext) {
         CacheEntry entry = ((SingleKeyNonTxInvocationContext)ctx).getCacheEntry();
         commitSerialEntryIfNeeded(ctx, skipOwnershipCheck, entry, commitVersion);
      } else {
         Set<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.iterator();
         final Log log = getLog();
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            if (!commitSerialEntryIfNeeded(ctx, skipOwnershipCheck, entry, commitVersion)) {
               if (trace) {
                  if (entry==null)
                     log.tracef("Entry for key %s is null : not calling commitUpdate", e.getKey());
                  else
                     log.tracef("Entry for key %s is not changed(%s): not calling commitUpdate", e.getKey(), entry);
               }
            }
         }
      }
   }

   @Override
   public void addTransaction(VersionVC commitVC) {
      dataContainer.addNewCommittedTransaction(commitVC);
   }

   protected boolean commitSerialEntryIfNeeded(InvocationContext ctx, boolean skipOwnershipCheck, CacheEntry entry,
                                               VersionVC commitVersion) {
      if (entry != null && entry.isChanged()) {
         if(entry instanceof SerializableEntry) {
            ((SerializableEntry) entry).commit(dataContainer, commitVersion);
         } else {
            entry.commit(dataContainer, null);
         }
         log.tracef("Committed entry %s", entry);
         return true;
      }
      return false;
   }

   protected boolean isKeyLocal(Object key) {
      return true;
   }

   private void validateReadSet(TxInvocationContext ctx, SerializablePrepareCommand command) throws InterruptedException {
      VersionVC toCompare = command.getVersion();
      Set<Object> readSet = new HashSet<Object>(Arrays.asList(command.getReadSet()));
      
      if (ctx.isOriginLocal()) {
         readSet.addAll(Arrays.asList(((LocalTransaction)ctx.getCacheTransaction()).getLocalReadSet()));
      }
      
      for (Object key : readSet) {
         if(isKeyLocal(key) && !dataContainer.validateKey(key, toCompare)) {
            throw new ValidationException("Validation of key [" + key + "] failed!", key);
         }         
      }      
   }
}
