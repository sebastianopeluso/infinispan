package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareResponseCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.totalorder.DistributedTotalOrderManager;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Created to control the total order validation. It disable the possibility of acquiring locks during execution through
 * the cache API
 *
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistTotalOrderInterceptor extends TotalOrderInterceptor {

   private static final Log log = LogFactory.getLog(DistTotalOrderInterceptor.class);
   private boolean trace;

   private DistributedTotalOrderManager totalOrderManager;

   @Inject
   public void inject(DistributedTotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
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
      totalOrderManager.addVersions(command.getGlobalTransaction(), command.getResult(), command.isException());
      return null;
   }
}
