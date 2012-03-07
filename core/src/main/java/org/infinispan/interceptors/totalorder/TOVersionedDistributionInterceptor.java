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
public class TOVersionedDistributionInterceptor extends VersionedDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TOVersionedDistributionInterceptor.class);

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //we have no locking interceptor and this set of affected keys is never populated! this solve
      //the problem (locate keys is returning a empty list of address!!)
      ctx.addAllAffectedKeys(command.getAffectedKeys());
      return super.visitPrepareCommand(ctx, command);
   }

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command,
                                         Collection<Address> recipients, boolean sync) {      

      boolean trace = log.isTraceEnabled();
      String globalTransactionString = Util.prettyPrintGlobalTransaction(command.getGlobalTransaction());

      if(trace) {
         log.tracef("Total Order Multicast transaction %s with Total Order", globalTransactionString);
      }
      
      if (!(command instanceof VersionedPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, ctx);
      rpcManager.invokeRemotely(recipients, command, false);      
   }

   @Override
   public Object visitPrepareResponseCommand(TxInvocationContext ctx, PrepareResponseCommand command) throws Throwable {
      Collection<Address> dest = Collections.singleton(command.getGlobalTransaction().getAddress());
      Object retVal = invokeNextInterceptor(ctx, command);
      rpcManager.invokeRemotely(dest, command, false);
      return retVal;
   }
}
