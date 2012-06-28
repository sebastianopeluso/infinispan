package org.infinispan.reconfigurableprotocol.protocol;

import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderReplicationInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderStateTransferLockInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedReplicationInterceptor;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.EnumMap;

import static org.infinispan.interceptors.InterceptorChain.InterceptorType;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class TotalOrderCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "TO";

   @Override
   public String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public boolean switchTo(ReconfigurableProtocol protocol) {
      return false;
   }

   @Override
   public void stopProtocol() throws InterruptedException {
      globalStopProtocol(true);
   }

   @Override
   public void bootProtocol() {
      //no-op
   }

   @Override
   public boolean canProcessOldTransaction(GlobalTransaction globalTransaction) {
      return false;
   }

   @Override
   public void bootstrapProtocol() {
      // TODO: Customise this generated block
   }

   @Override
   public EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      EnumMap<InterceptorType, CommandInterceptor> interceptors = buildDefaultInterceptorChain();

      //State transfer
      interceptors.put(InterceptorType.STATE_TRANSFER,
                       createInterceptor(new TotalOrderStateTransferLockInterceptor(),
                                         TotalOrderStateTransferLockInterceptor.class));

      //Custom interceptor
      interceptors.put(InterceptorType.CUSTOM_INTERCEPTOR_BEFORE_TX_INTERCEPTOR,
                       createInterceptor(new TotalOrderInterceptor(), TotalOrderInterceptor.class));

      //No locking
      interceptors.remove(InterceptorType.LOCKING);

      //Wrapper
      if (configuration.versioning().enabled() && configuration.clustering().cacheMode().isClustered()) {
         interceptors.put(InterceptorType.WRAPPER,
                          createInterceptor(new TotalOrderVersionedEntryWrappingInterceptor(),
                                            TotalOrderVersionedEntryWrappingInterceptor.class));
      }

      //No deadlock
      interceptors.remove(InterceptorType.DEADLOCK);

      //Clustering
      switch (configuration.clustering().cacheMode()) {
         case REPL_SYNC:
            if (configuration.versioning().enabled()) {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderVersionedReplicationInterceptor(),
                                                  TotalOrderVersionedReplicationInterceptor.class));
               break;
            } else {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderReplicationInterceptor(),
                                                  TotalOrderReplicationInterceptor.class));
               break;
            }
         case DIST_SYNC:
            if (configuration.versioning().enabled()) {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderVersionedDistributionInterceptor(),
                                                  TotalOrderVersionedDistributionInterceptor.class));
               break;
            } else {
               interceptors.put(InterceptorType.CLUSTER,
                                createInterceptor(new TotalOrderDistributionInterceptor(),
                                                  TotalOrderDistributionInterceptor.class));
               break;
            }
      }

      return interceptors;
   }

   @Override
   public boolean use1PC(LocalTransaction localTransaction) {
      return !configuration.versioning().enabled() || 
            (configuration.transaction().useSynchronization() && !configuration.clustering().cacheMode().isDistributed());
   }

   @Override
   protected void internalHandleData(Object data, Address from) {
      //no-op
   }
}
