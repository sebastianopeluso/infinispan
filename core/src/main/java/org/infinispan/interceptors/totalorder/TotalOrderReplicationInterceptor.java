package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.transaction.totalorder.TotalOrderManager;

/**
 * @author mircea.markus@jboss.com
 * @since 5.2.0
 */
public class TotalOrderReplicationInterceptor extends ReplicationInterceptor {

   private TotalOrderManager totalOrderManager;

   @Inject
   public void init(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object result = super.visitPrepareCommand(ctx, command);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         // Because StateTransferLockInterceptor already acquired the state transfer lock on this thread,
         // we could get in a deadlock with state transfer while waiting for the total order thread to do its work.
         // So we release the state transfer lock while waiting, even though it's probably not optimal.
         stateTransferLock.releaseForCommand(ctx, command);

         //we need to do the waiting here and not in the TotalOrderInterceptor because it is possible for the replication
         //not to take place, e.g. in the case there are no changes in the context. And this is the place where we know
         // if the replication occurred.
         totalOrderManager.waitForPrepareToSucceed(ctx);

         stateTransferLock.acquireForCommand(ctx, command);
      }
      return result;
   }
}
