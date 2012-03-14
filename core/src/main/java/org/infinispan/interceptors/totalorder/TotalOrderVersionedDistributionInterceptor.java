package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.PrepareResponseCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.VersionedDistributionInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
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

      recipients.add(rpcManager.getAddress());
      setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, ctx);
      rpcManager.invokeRemotely(recipients, command, false, false, true);      
   }

   @Override
   public Object visitPrepareResponseCommand(TxInvocationContext ctx, PrepareResponseCommand command) throws Throwable {
      Address destination = command.getGlobalTransaction().getAddress();
      Collection<Address> destinationList = Collections.singleton(destination);
      Object retVal = invokeNextInterceptor(ctx, command);
      
      if (!destination.equals(rpcManager.getAddress())) {
         //don't send to myself
         rpcManager.invokeRemotely(destinationList, command, false);
      }
      return retVal;
   }
}
