package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.remoting.RpcException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author mircea.markus@jboss.com
 * @since 5.2.0
 */
public class TotalOrderReplicationInterceptor extends ReplicationInterceptor {
   
   private static final Log log = LogFactory.getLog(TotalOrderReplicationInterceptor.class);
   static final boolean  trace = log.isTraceEnabled();
   private boolean isSyncCache;

   @Start
   public void start() {
      isSyncCache = configuration.getCacheMode().isSynchronous();
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object result = super.visitPrepareCommand(ctx, command);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         //we need to do the waiting here and not in the TotalOrderInterceptor because it is possible for the replication
         //not to take place, e.g. in the case there are no changes in the context. And this is the place where we know
         // if the replication occurred.
         waitForPrepareToSucceed(ctx);
      }
      return result;
   }

   private void waitForPrepareToSucceed(TxInvocationContext context) {
      if (!context.isOriginLocal()) throw new IllegalStateException();

      if (isSyncCache) {

         //in sync mode, blocks in the LocalTransaction
         logWaiting(context);

         LocalTransaction localTransaction = (LocalTransaction) context.getCacheTransaction();
         try {
            localTransaction.awaitUntilModificationsApplied(configuration.getSyncReplTimeout());
         } catch (Throwable throwable) {
            throw new RpcException(throwable);
         } finally {
            if (trace)
               log.tracef("Transaction [%s] finishes the waiting time", context.getGlobalTransaction().prettyPrint());
         }
      }
   }

   private void logWaiting(TxInvocationContext context) {
      if (trace) {
         log.tracef("Transaction [%s] sent in synchronous mode. waiting until modification is applied",
                    context.getGlobalTransaction().prettyPrint());
      }
   }
}
