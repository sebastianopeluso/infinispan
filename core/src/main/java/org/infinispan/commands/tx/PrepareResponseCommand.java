package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PrepareResponseCommand extends AbstractTransactionBoundaryCommand {

   public static final byte COMMAND_ID = 100;

   private Throwable exception;
   private Set<Object> keysValidated;

   public PrepareResponseCommand(String cacheName) {
      super(cacheName);
   }

   public PrepareResponseCommand(String cacheName, GlobalTransaction gtx) {
      super(cacheName);
      this.globalTx = gtx;
   }

   public void setException(Throwable exception) {
      this.exception = exception;
   }

   public void setKeysValidated(Set<Object> keysValidated) {
      if (keysValidated == null || keysValidated.isEmpty()) {
         return;
      }
      this.keysValidated = keysValidated;
   }

   public Throwable getException() {
      return exception;
   }

   public Set<Object> getKeysValidated() {
      return keysValidated;
   }

   @Override
   public Object perform(InvocationContext context) throws Throwable {
      if (context != null) {
         throw new IllegalStateException("Expected null context!");
      }

      globalTx.setRemote(true);
      RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);

      //remote transaction can be null. However, it is not needed while processing this command
      RemoteTxInvocationContext ctx = icc.createRemoteTxInvocationContext(transaction, getOrigin());

      return invoker.invoke(ctx, this);
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx, exception, keysValidated};
   }

   @SuppressWarnings({"unchecked"})
   @Override
   public void setParameters(int commandId, Object[] args) {
      this.globalTx = (GlobalTransaction) args[0];
      this.exception = (Throwable) args[1];
      this.keysValidated = (Set<Object>) args[2];
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPrepareResponseCommand((TxInvocationContext) ctx, this);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "PrepareResponseCommand{" +
            "exception=" + exception +
            ", keysValidated=" + keysValidated +
            ", " + super.toString();
   }
}
