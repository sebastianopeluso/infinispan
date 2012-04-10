package org.infinispan.tx.totalorder;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.tx.recovery.RecoveryDummyTransactionManagerLookup;
import org.infinispan.tx.recovery.RecoveryWithDefaultCacheReplTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.totalorder.RecoveryWithDefaultCacheTest")
@CleanupAfterMethod
public class RecoveryWithDefaultCacheTest extends RecoveryWithDefaultCacheReplTest {

   private ConfigurationBuilder dcc;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = configureBuilder();
      createCluster(dcc, 2);
      waitForClusterToForm();

      //check that a default cache has been created
      manager(0).getCacheNames().contains(getRecoveryCacheName());
      manager(1).getCacheNames().contains(getRecoveryCacheName());
   }

   protected ConfigurationBuilder configureBuilder() {
      dcc = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
      dcc.clustering().stateTransfer().fetchInMemoryState(false)
            .transaction().transactionManagerLookup(new RecoveryDummyTransactionManagerLookup())
            .recovery();
      return dcc;
   }

   @Override
   protected void addAnotherCache() {
      addClusterEnabledCacheManager(dcc);
      defineRecoveryCache(1);
   }
}
