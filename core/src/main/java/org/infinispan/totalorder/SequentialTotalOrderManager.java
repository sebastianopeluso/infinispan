package org.infinispan.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;

/**
 * @author mircea.markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.2.0
 */
@MBean(objectName = "TotalOrderManager", description = "Simple total order management")
public class SequentialTotalOrderManager extends BaseTotalOrderManager {

   public final void validateTransaction(PrepareCommand prepareCommand, TxInvocationContext ctx, CommandInterceptor invoker) {
      if (trace)
         log.tracef("Validate transaction %s", prepareCommand.getGlobalTransaction().prettyPrint());

      if (ctx.isOriginLocal()) throw new IllegalArgumentException("Local invocation not allowed!");

      Object result = null;
      boolean exception = false;
      long startTime = now();
      try {
         result = prepareCommand.acceptVisitor(ctx, invoker);
      } catch (Throwable t) {
         log.trace("Exception while processing the rest of the interceptor chain", t);
         result = t;
         exception = true;
      } finally {
         logValidationFinalStatus(prepareCommand, result, exception);
         updateLocalTransaction(result, exception, prepareCommand);
         updateValidationDurationStats(startTime, now());
      }
   }

   private void updateValidationDurationStats(long start, long end) {
      if (statisticsEnabled) {
         validationDuration.addAndGet(end - start);
         numberOfTxValidated.incrementAndGet();
      }
   }


   private void logValidationFinalStatus(PrepareCommand prepareCommand, Object result, boolean exception) {
      if (trace)
         log.tracef("Transaction %s finished validation (%s). Validation result is %s ",
                    prepareCommand.getGlobalTransaction().prettyPrint(),
                    (exception ? "failed" : "ok"), (exception ? ((Throwable) result).getMessage() : result));
   }
}
