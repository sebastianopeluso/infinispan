package org.infinispan.tx.gmu;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistConsistencyTest extends ConsistencyTest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
