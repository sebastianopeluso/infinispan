package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.PrepareResponseCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.VersionedDistributionInterceptor;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;

import static org.infinispan.transaction.WriteSkewHelper.readVersionsFromResponse;
import static org.infinispan.transaction.WriteSkewHelper.setVersionsSeenOnPrepareCommand;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class TODistributionInterceptor extends VersionedDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TODistributionInterceptor.class);

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

      setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, ctx);
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
            Object retVal = localTransaction.awaitUntilModificationsApplied();
            if (retVal instanceof EntryVersionsMap) {
                readVersionsFromResponse(SuccessfulResponse.create(retVal), ctx.getCacheTransaction());
            } else {
                throw new IllegalStateException("This must not happen! we must receive the versions");
            }
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
   public Object visitPrepareResponseCommand(TxInvocationContext ctx, PrepareResponseCommand command) throws Throwable {
      Collection<Address> dest = Collections.singleton(command.getGlobalTransaction().getAddress());
      Object retVal = invokeNextInterceptor(ctx, command);
      rpcManager.invokeRemotely(dest, command, false);
      return retVal;
   }
}
