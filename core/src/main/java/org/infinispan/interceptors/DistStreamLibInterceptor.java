package org.infinispan.interceptors;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * Distributed Stream Lib Interceptor that knows if a key is local or remote
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistStreamLibInterceptor extends StreamLibInterceptor {
   private DistributionManager distributionManager;

   @Inject
   public void inject(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   @Override
   protected boolean isRemote(Object k) {
      return !distributionManager.getLocality(k).isLocal();
   }
}
