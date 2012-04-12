package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.VersionedReplicationInterceptor;
import org.infinispan.totalorder.TotalOrderManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.transaction.WriteSkewHelper.setVersionsSeenOnPrepareCommand;

/**
 * Replication Interceptor for Total Order protocol with versioning.
 *
 * @author Pedro Ruivo
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class TotalOrderVersionedReplicationInterceptor extends VersionedReplicationInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedReplicationInterceptor.class);

   private TotalOrderManager totalOrderManager;

   @Inject
   public void init(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   protected void broadcastPrepare(TxInvocationContext ctx, PrepareCommand command) {

      if (!(command instanceof VersionedPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      if (log.isTraceEnabled())
         log.tracef("Broadcasting prepare for transaction %s with total order", command.getGlobalTransaction());

      setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, ctx);
      //broadcast the command
      boolean sync = configuration.isTransactionRecoveryEnabled();
      rpcManager.broadcastRpcCommand(command, sync, true);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         //we need to do the waiting here and not in the TotalOrderInterceptor because it is possible for the replication
         //not to take place, e.g. in the case there are no changes in the context. And this is the place where we know
         // if the replication occurred.
         totalOrderManager.waitForPrepareToSucceed(ctx);
      }
   }
}
