package org.infinispan.distribution.wrappers;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * Websiste: www.cloudtm.eu
 * Date: 02/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class DistCustomStatsInterceptor extends CustomStatsInterceptor {

   private DistributionManager distributionManager;

   @Inject
   public void inject(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   @Override
   public boolean isRemote(Object key) {
      return !distributionManager.getLocality(key).isLocal();
   }
}
