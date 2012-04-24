package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DistributionInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.Util;

import java.util.Arrays;
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
      ctx.addAllAffectedKeys(Util.getAffectedKeys(Arrays.asList(command.getModifications()), dataContainer));
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
      rpcManager.invokeRemotely(recipients, command, false, false, true);
   }
}
