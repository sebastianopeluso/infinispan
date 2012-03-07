package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PrepareResponseCommand extends AbstractTransactionBoundaryCommand {

   public static final byte COMMAND_ID = 100;

   private boolean exception;
   private Object result;

   public PrepareResponseCommand(String cacheName) {
      super(cacheName);     
   }

   public PrepareResponseCommand(String cacheName, GlobalTransaction gtx) {
      super(cacheName);
      this.globalTx = gtx;      
   }

   public void addException(Object exception) {
      this.exception = true;
      this.result = exception;
   }

   public void addVersions(EntryVersionsMap versionsMap) {
      this.exception = false;
      this.result = versionsMap;
   }

   public boolean isException() {
      return exception;
   }

   public Object getResult() {
      return result;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx, exception, result};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      this.globalTx = (GlobalTransaction) args[0];
      this.exception = (Boolean) args[1];
      this.result = args[2];
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
            ", result=" + result +
            super.toString();
   }
}
