package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.remoting.RpcException;
import org.infinispan.totalorder.TotalOrderManager;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
         //we need to do the waiting here and not in the TotalOrderInterceptor because it is possible for the replication
         //not to take place, e.g. in the case there are no changes in the context. And this is the place where we know
         // if the replication occurred.
         totalOrderManager.waitForPrepareToSucceed(ctx);
      }
      return result;
   }
}
