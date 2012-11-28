package org.infinispan.tx.gmu;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistConsistencyTest2 extends ConsistencyTest {

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }

   @Override
   protected int initialClusterSize() {
      return 5;
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(2);
   }
}
