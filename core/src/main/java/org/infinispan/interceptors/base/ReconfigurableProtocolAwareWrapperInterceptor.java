package org.infinispan.interceptors.base;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class ReconfigurableProtocolAwareWrapperInterceptor extends CommandInterceptor {

   private final ConcurrentMap<String, CommandInterceptor> protocolDependentInterceptor = new ConcurrentHashMap<String, CommandInterceptor>();

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   /**
    * set the command interceptor that a transaction boundary command created in a specific transaction protocol
    * should visit
    * @param protocolId    the transaction protocol identifier
    * @param interceptor   the command interceptor
    */
   public final void setProtocolDependentInterceptor(String protocolId, CommandInterceptor interceptor) {
      protocolDependentInterceptor.put(protocolId, interceptor);
      interceptor.setNext(getNext());
   }

   /**
    * initialize the internal interceptor setting their next command interceptor
    */
   public final void initializeInternalInterceptors() {
      for (CommandInterceptor commandInterceptor : protocolDependentInterceptor.values()) {
         commandInterceptor.setNext(getNext());
      }
   }

   private CommandInterceptor getNext(GlobalTransaction globalTransaction) {
      String protocolId = globalTransaction.getProtocolId();
      CommandInterceptor next = protocolDependentInterceptor.get(protocolId);
      return next != null ? next : getNext();
   }

   private Object handleTxBoundaryCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      return command.acceptVisitor(ctx, getNext(ctx.getGlobalTransaction()));
   }

}
