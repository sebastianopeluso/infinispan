package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.StateTransferLockInterceptor;
import org.infinispan.statetransfer.StateTransferInProgressException;
import org.infinispan.statetransfer.StateTransferLockReacquisitionException;
import org.infinispan.transaction.totalorder.TotalOrderManager;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
public class TotalOrderStateTransferLockInterceptor extends StateTransferLockInterceptor {

   private TotalOrderManager tom;

   @Inject
   public void init(TotalOrderManager tom) {
      this.tom = tom;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         super.visitPrepareCommand(ctx, command);
      } catch (Throwable throwable) {
         if (!ctx.isOriginLocal())
            tom.notifyStateTransferInProgress(command.getGlobalTransaction(), throwable);
         throw throwable;
      }

      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         signalStateTransferInProgress(ctx);
      }

      boolean release = true;
      try {
         return handleWithRetries(ctx, command, rpcTimeout);
      } catch (StateTransferLockReacquisitionException e) {
         release = false;
         return signalStateTransferInProgress(ctx);
      } finally {
         if (release) {
            stateTransferLock.releaseForCommand(ctx, command);
         }
      }
   }
}
