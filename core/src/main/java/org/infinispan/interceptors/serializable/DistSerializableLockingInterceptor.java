package org.infinispan.interceptors.serializable;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/** 
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class DistSerializableLockingInterceptor extends SerializableLockingInterceptor {

   private DistributionManager distributionManager;

   @Inject
   public void inject(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   @Override
   protected Set<Object> getOnlyLocalKeys(Collection<Object> keys) {
      Set<Object> localKeys = new HashSet<Object>();
      for (Object key : keys) {
         if (isKeyLocal(key)) {
            localKeys.add(key);
         }
      }
      return localKeys;
   }

   @Override
   protected boolean isKeyLocal(Object key) {
      return distributionManager.getLocality(key).isLocal();
   }
}
