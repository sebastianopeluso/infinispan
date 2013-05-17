package org.infinispan.tx.gmu.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.Random;
import java.util.concurrent.Future;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional")
public abstract class BaseConsistencyTest extends MultipleCacheManagersTest {

   private static final int NUM_OF_ACCOUNTS = 100;
   private static final int INITIAL_AMOUNT = 10000;
   protected final int clusterSize;
   protected final CacheMode cacheMode;

   protected BaseConsistencyTest(int clusterSize, CacheMode cacheMode) {
      this.clusterSize = clusterSize;
      this.cacheMode = cacheMode;
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   public final void testJoin() throws Exception {
      populate();
      assertInitialValues();
      sanityCheck();
      assertTrue("Needs a larger cluster size", clusterSize >= 2);

      TransferMoney writer = new TransferMoney(this.<String, Integer>cache(0), tm(0));
      ConsistencyCheck reader = new ConsistencyCheck(this.<String, Integer>cache(1), tm(1));

      Future<Object> writeFuture = fork(writer, null);
      Future<Object> readerFuture = fork(reader, null);

      addClusterEnabledCacheManager(createConfiguration());
      TestingUtil.waitForRehashToComplete(caches());

      writer.stopTransfers();
      reader.stopChecks();

      writeFuture.get();
      readerFuture.get();

      assertEquals("No errors expected!", 0, reader.errors());
      sanityCheck();
   }

   public final void testLeave() throws Exception {
      populate();
      assertInitialValues();
      sanityCheck();
      assertTrue("Needs a larger cluster size", clusterSize >= 3);

      TransferMoney writer = new TransferMoney(this.<String, Integer>cache(0), tm(0));
      ConsistencyCheck reader = new ConsistencyCheck(this.<String, Integer>cache(1), tm(1));

      Future<Object> writeFuture = fork(writer, null);
      Future<Object> readerFuture = fork(reader, null);

      TestingUtil.killCacheManagers(cacheManagers.remove(2));
      TestingUtil.waitForRehashToComplete(caches());

      writer.stopTransfers();
      reader.stopChecks();

      writeFuture.get();
      readerFuture.get();

      assertEquals("No errors expected!", 0, reader.errors());
      sanityCheck();
   }

   @Override
   protected final void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = createConfiguration();
      createClusteredCaches(clusterSize, builder);
   }

   protected final ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.locking().lockAcquisitionTimeout(100).isolationLevel(IsolationLevel.SERIALIZABLE)
            .clustering().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(true)
            .hash().numOwners(2).numSegments(50)
            .versioning().enable().scheme(VersioningScheme.GMU);
      return builder;
   }

   protected final void safeRollback(TransactionManager transactionManager) {
      try {
         transactionManager.rollback();
      } catch (Throwable t) {
         //ignore!
      }
   }

   protected final String generateRandomAccount(Random random) {
      return getAccount(random.nextInt(NUM_OF_ACCOUNTS));
   }

   protected final String getAccount(int index) {
      return "ACC_" + index;
   }

   protected final int generateRandomAmount(Random random, int max) {
      return max <= 1 ? 0 : random.nextInt(max - 1);
   }

   private void populate() {
      final Cache<String, Integer> cache = cache(0);
      final TransactionManager transactionManager = tm(0);
      boolean success = false;
      do {
         try {
            transactionManager.begin();
            for (int i = 0; i < NUM_OF_ACCOUNTS; ++i) {
               cache.put(getAccount(i), INITIAL_AMOUNT);
            }
            transactionManager.commit();
            success = true;
         } catch (Exception e) {
            safeRollback(transactionManager);
         }
      } while (!success);
   }

   private void assertInitialValues() {
      for (final Cache<String, Integer> cache : this.<String, Integer>caches()) {
         for (int i = 0; i < NUM_OF_ACCOUNTS; ++i) {
            int value = cache.get(getAccount(i));
            assertEquals("Wrong initial value.", INITIAL_AMOUNT, value);
         }
      }
   }

   private void sanityCheck() {
      final int totalAmount = INITIAL_AMOUNT * NUM_OF_ACCOUNTS;
      for (final Cache<String, Integer> cache : this.<String, Integer>caches()) {
         final TransactionManager transactionManager = tm(cache);
         int sum = 0;
         try {
            transactionManager.begin();
            for (int i = 0; i < NUM_OF_ACCOUNTS; ++i) {
               sum += cache.get(getAccount(i));
            }
            transactionManager.commit();
         } catch (Exception e) {
            safeRollback(transactionManager);
         }
         assertEquals("Consistency check failed!", totalAmount, sum);
      }
   }

   private class TransferMoney implements Runnable {

      private final Cache<String, Integer> cache;
      private final TransactionManager transactionManager;
      private final Random random;
      private volatile boolean running = false;

      private TransferMoney(Cache<String, Integer> cache, TransactionManager transactionManager) {
         this.cache = cache;
         this.transactionManager = transactionManager;
         this.random = new Random();
      }

      @Override
      public final void run() {
         running = true;
         while (running) {
            String srcAccount = generateRandomAccount(random);
            String dstAccount = generateRandomAccount(random);
            try {
               transactionManager.begin();
               Integer src = cache.get(srcAccount);
               Integer transfer = generateRandomAmount(random, src);
               Integer dst = cache.get(dstAccount);

               src -= transfer;
               dst += transfer;

               cache.put(srcAccount, src);
               cache.put(dstAccount, dst);
               transactionManager.commit();
            } catch (Exception e) {
               safeRollback(transactionManager);
            }
         }
      }

      public final void stopTransfers() {
         running = false;
      }
   }

   private class ConsistencyCheck implements Runnable {

      private final Cache<String, Integer> cache;
      private final TransactionManager transactionManager;
      private final int totalAmount;
      private volatile boolean running = false;
      private volatile int errors;

      private ConsistencyCheck(Cache<String, Integer> cache, TransactionManager transactionManager) {
         this.cache = cache;
         this.transactionManager = transactionManager;
         this.totalAmount = INITIAL_AMOUNT * NUM_OF_ACCOUNTS;
         this.errors = 0;
      }

      @Override
      public final void run() {
         running = true;
         while (running) {
            int sum = 0;
            try {
               transactionManager.begin();
               for (int i = 0; i < NUM_OF_ACCOUNTS; ++i) {
                  sum += cache.get(getAccount(i));
               }

               transactionManager.commit();
            } catch (Exception e) {
               safeRollback(transactionManager);
            }
            if (sum != totalAmount) {
               errors++;
            }
         }
      }

      public final void stopChecks() {
         running = false;
      }

      public final int errors() {
         return errors;
      }
   }
}
