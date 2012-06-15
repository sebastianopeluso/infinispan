package org.infinispan.interceptors.serializable;

import org.infinispan.CacheException;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.SerializableCommitCommand;
import org.infinispan.commands.tx.SerializablePrepareCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SerializableTxInterceptor extends TxInterceptor {

   private static final Log log = LogFactory.getLog(SerializableTxInterceptor.class);

   protected CommitQueue commitQueue;
   protected VersionVCFactory versionVCFactory;

   @Inject
   public void inject(CommitQueue commitQueue, VersionVCFactory versionVCFactory) {
      this.commitQueue = commitQueue;
      this.versionVCFactory = versionVCFactory;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!(command instanceof SerializablePrepareCommand)) {
         throw new IllegalArgumentException("Expected Serializable Prepare Command but it is " + command.getClass().getSimpleName());
      }

      if (ctx.isOriginLocal()) {
         initializeSerializablePrepareCommand(ctx, (SerializablePrepareCommand) command);
      }

      return super.visitPrepareCommand(ctx, command);      
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!(command instanceof SerializableCommitCommand)) {
         throw new IllegalArgumentException("Expected Serializable Commit Command but it is " + command.getClass().getSimpleName());
      }

      if (ctx.isOriginLocal()) {
         initializeSerializableCommitCommand(ctx, (SerializableCommitCommand) command);
      }

      return super.visitCommitCommand(ctx, command);    // TODO: Customise this generated block
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object retVal = super.visitGetKeyValueCommand(ctx, command);
      return afterSuccessfullyRead(ctx, command, retVal);
   }

   protected void initializeSerializablePrepareCommand(TxInvocationContext ctx, SerializablePrepareCommand command) {
      command.setReadSet(((LocalTransaction)ctx.getCacheTransaction()).getLocalReadSet());
      command.setVersion(ctx.getPrepareVersion());
   }

   @Override
   protected Object afterSuccessfullyPrepared(TxInvocationContext ctx, PrepareCommand command, Object retVal) {
      VersionVC prepareVC = commitQueue.addTransaction(command.getGlobalTransaction(), ctx.calculateVersionToRead(this.versionVCFactory),
                                                       ctx.clone());
      VersionVC commitVC;
      if(ctx.isOriginLocal()) {
         commitVC = ((LocalTransaction) ctx.getCacheTransaction()).getCommitVersion();
         if (commitVC == null) {
            ((LocalTransaction) ctx.getCacheTransaction()).setCommitVersion(prepareVC);
         } else {
            commitVC.setToMaximum(prepareVC);
         }
      } else {
         commitVC = prepareVC;
      }


      log.debugf("transaction [%s] commit vector clock is %s",
                 command.getGlobalTransaction().prettyPrint(), commitVC);
      return commitVC;
   }

   protected Object afterSuccessfullyRead(InvocationContext ctx, GetKeyValueCommand command, Object valueRead) {
      if (!ctx.isInTxScope()) {
         return valueRead;
      }
      TxInvocationContext tctx = (TxInvocationContext) ctx;
      tctx.markReadFrom(0);
      //update vc
      InternalMVCCEntry ime = ctx.getLocalReadKey(command.getKey());
      if(ime == null) {
         log.warn("InternalMVCCEntry is null.");
      } else if(tctx.hasModifications() && !ime.isMostRecent()) {
         //read an old value... the tx will abort in commit,
         //so, do not waste time and abort it now
         throw new CacheException("transaction must abort!! read an old value and it is not a read only transaction");
      } else {
         VersionVC v = ime.getVersion();
         if(this.versionVCFactory.translateAndGet(v,0) == VersionVC.EMPTY_POSITION) {
            this.versionVCFactory.translateAndSet(v,0,0);

         }
         tctx.updateVectorClock(v);
      }
      return valueRead;
   }

   private void initializeSerializableCommitCommand(TxInvocationContext ctx, SerializableCommitCommand command) {
      command.setCommitVersion(((LocalTransaction)ctx.getCacheTransaction()).getCommitVersion());
   }
}
