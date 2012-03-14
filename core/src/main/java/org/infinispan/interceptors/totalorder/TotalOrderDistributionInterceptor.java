package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DistributionInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.totalorder.TotalOrderManager;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderDistributionInterceptor extends DistributionInterceptor {
   
   private TotalOrderManager totalOrderManager;
   
   @Inject
   public void injectDependencies(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object result = super.visitPrepareCommand(ctx, command);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         //we need to do the waiting here and not in the TotalOrderInterceptor because it is possible for the replication
         //not to take place, e.g. in the case there are no changes in the context. And this is the place where we know
         // if the replication occurred.
         totalOrderManager.waitForPrepareToSucceed(ctx);
      }
      return result;
   }

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      //we need to add myself to the recipients list
      recipients.add(rpcManager.getAddress());
      super.prepareOnAffectedNodes(ctx, command, recipients, sync);
   }
}
