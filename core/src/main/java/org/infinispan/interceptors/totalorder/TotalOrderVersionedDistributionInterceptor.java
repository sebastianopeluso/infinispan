package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.PrepareResponseCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.VersionedDistributionInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;

import static org.infinispan.transaction.WriteSkewHelper.setVersionsSeenOnPrepareCommand;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderVersionedDistributionInterceptor extends VersionedDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedDistributionInterceptor.class);

   private TotalOrderManager totalOrderManager;

   @Inject
   public void injectDependencies(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      ctx.addAllAffectedKeys(command.getAffectedKeys());

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
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command,
                                         Collection<Address> recipients, boolean sync) {

      boolean trace = log.isTraceEnabled();

      if(trace) {
         log.tracef("Total Order Multicast transaction %s with Total Order", command.getGlobalTransaction().prettyPrint());
      }

      if (!(command instanceof VersionedPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, ctx);
      rpcManager.invokeRemotely(recipients, command, false, false, true);
   }

   @Override
   public Object visitPrepareResponseCommand(TxInvocationContext ctx, PrepareResponseCommand command) throws Throwable {
      boolean trace = log.isTraceEnabled();

      Address destination = command.getGlobalTransaction().getAddress();
      Collection<Address> destinationList = Collections.singleton(destination);
      Object retVal = invokeNextInterceptor(ctx, command);

      //don't send to myself
      if (!destination.equals(rpcManager.getAddress())) {
         if (trace) {
            log.tracef("Sending the Prepare Response %s to %s", command, destinationList);
         }
         rpcManager.invokeRemotely(destinationList, command, false, true, false);
      } else {
         if (trace) {
            log.tracef("Skip sending the Prepare Response %s because the destination [%s] is myself [%s]",
                       command, destinationList, rpcManager.getAddress());
         }
      }
      return retVal;
   }
}
