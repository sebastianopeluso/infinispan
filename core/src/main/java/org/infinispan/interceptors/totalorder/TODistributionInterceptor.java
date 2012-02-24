package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DistributionInterceptor;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class TODistributionInterceptor extends DistributionInterceptor {

   private static final Log log = LogFactory.getLog(TODistributionInterceptor.class);

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command,
                                         Collection<Address> recipients, boolean sync) {
      if (!command.isTotalOrdered()) {
         super.prepareOnAffectedNodes(ctx, command, recipients, sync);
         return ;
      }

      boolean trace = log.isTraceEnabled();
      String globalTransactionString = Util.prettyPrintGlobalTransaction(command.getGlobalTransaction());

      if(trace) {
         log.tracef("Total Order Multicast transaction %s with Total Order", globalTransactionString);
      }

      rpcManager.invokeRemotely(recipients, command, false);

      if(sync) {
         //in sync mode, blocks in the LocalTransaction
         if(trace) {
            log.tracef("Transaction [%s] sent in synchronous mode. waiting until modification is applied",
                  globalTransactionString);
         }
         //this is only invoked in local context
         LocalTransaction localTransaction = (LocalTransaction) ctx.getCacheTransaction();
         try {
            localTransaction.awaitUntilModificationsApplied(configuration.getSyncReplTimeout());
         } catch (Throwable throwable) {
            throw new RpcException(throwable);
         } finally {
            if(trace) {
               log.tracef("Transaction [%s] finishes the waiting time",
                     globalTransactionString);
            }
         }
      }

   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (command.shouldInvokedRemotely()) {
         return super.visitRollbackCommand(ctx, command);
      }
      return invokeNextInterceptor(ctx, command);
   }

}
