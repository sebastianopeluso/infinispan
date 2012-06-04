package org.infinispan.interceptors.serializable;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.mvcc.VersionVC;

/**
 *
 * @author Pedro Ruivo  
 * @since 5.2
 */
public class DistSerializableEntryWrappingInterceptor extends SerializableEntryWrappingInterceptor {

   private DistributionManager distributionManager;

   @Inject
   public void inject(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   @Override
   protected boolean commitSerialEntryIfNeeded(InvocationContext ctx, boolean skipOwnershipCheck, CacheEntry entry, VersionVC commitVersion) {      
      if (entry == null) {
         return false;
      }
      
      boolean doCommit = true;
      // ignore locality for removals, even if skipOwnershipCheck is not true
      if (!skipOwnershipCheck && !entry.isRemoved() && !isKeyLocal(entry.getKey())) {
         if (configuration.isL1CacheEnabled()) {
            distributionManager.transformForL1(entry);
         } else {
            doCommit = false;
         }
      }      
      if (doCommit && entry.isChanged()) {
         if(entry instanceof SerializableEntry) {
            ((SerializableEntry) entry).commit(dataContainer, commitVersion);
         } else {
            entry.commit(dataContainer, null);
         }
      } else {
         entry.rollback();
      }
      return doCommit;
   }

   @Override
   protected boolean isKeyLocal(Object key) {
      return distributionManager.getLocality(key).isLocal();
   }
}
