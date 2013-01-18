package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Clustered with Write Skew check environment
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "atomic.WriteSkewClusteredAPITest")
public class WriteSkewClusteredAPITest extends ClusteredAPITest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.invocationBatching().enable(true)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.OPTIMISTIC)
            .versioning().enabled(true).scheme(VersioningScheme.SIMPLE);

      createClusteredCaches(2, "atomic", builder);
   }
}
