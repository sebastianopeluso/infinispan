package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareResponseCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.totalorder.DistributedTotalOrderValidator;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Created to control the total order validation. It disable the possibility of acquiring locks during execution through
 * the cache API
 *
 * Created by IntelliJ IDEA.
 * Date: 1/15/12
 * Time: 9:48 PM
 *
 * @author Pedro Ruivo
 */
public class DistTotalOrderInterceptor extends TotalOrderInterceptor {

   private static final Log log = LogFactory.getLog(DistTotalOrderInterceptor.class);
   private boolean trace;

   private DistributedTotalOrderValidator totalOrderValidator;

   @Inject
   public void inject(DistributedTotalOrderValidator totalOrderValidator) {
      this.totalOrderValidator = totalOrderValidator;
   }

   @Start
   public void setLogLevel() {
      this.trace = log.isTraceEnabled();
   }

   @Override
   public Object visitPrepareResponseCommand(TxInvocationContext ctx, PrepareResponseCommand command) throws Throwable {
      if (trace) {
         log.tracef("Add response %s to transaction %s", command,
               Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
      }
      totalOrderValidator.addVersions(command.getGlobalTransaction(), command.getResult(), command.isException());
      return null;
   }
}
