package org.infinispan.dataplacement;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.dataplacement.hm.HashMapObjectLookupFactory;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.stats.topK.DistributedStreamSummaryInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", description = "something fancy")
public class DataPlacementTest extends MultipleCacheManagersTest {

   private static final int NUMBER_OF_KEYS = 1000;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.dataPlacement().enabled(true).objectLookupFactory(new HashMapObjectLookupFactory());
      builder.clustering().stateTransfer().fetchInMemoryState(true);
      builder.clustering().hash().numOwners(1).numSegments(1000);
      builder.customInterceptors().addInterceptor().before(TxInterceptor.class).interceptor(new DistributedStreamSummaryInterceptor());
      createCluster(builder, 2);
   }
   
   public void testKeySwap() {
      for (int i = 0; i < NUMBER_OF_KEYS; ++i) {
         cache(0).put(getKey(i), "1"); //dummy value
      }
      
      //TODO reset top key in both caches
      //TODO cache 0 will read from 0 to 499 and cache 1 will read from 500 to 1000
      //TODO trigger data placement: we are using the hash map to avoid errors in location
      //TODO check keys position (data container, consistent hash)
   }
   
   private String getKey(int i) {
      return "KEY_" + i;
   }
}
