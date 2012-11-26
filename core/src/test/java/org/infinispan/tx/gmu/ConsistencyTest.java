package org.infinispan.tx.gmu;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static junit.framework.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.ConsistencyTest")
public class ConsistencyTest extends AbstractGMUTest {

   private static final String COUNTER_1 = "counter_1";
   private static final String COUNTER_2 = "counter_2";

   public void testGetSnapshot() throws Exception {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2, KEY_3);

      cache(0).put(KEY_1, VALUE_1);
      cache(0).put(KEY_2, VALUE_1);
      cache(0).put(KEY_3, VALUE_1);

      assertCachesValue(0, KEY_1, VALUE_1);
      assertCachesValue(0, KEY_2, VALUE_1);
      assertCachesValue(0, KEY_3, VALUE_1);

      TransactionManager tm0 = cache(0).getAdvancedCache().getTransactionManager();
      tm0.begin();
      assert VALUE_1.equals(cache(0).get(KEY_1));
      Transaction tx0 = tm0.suspend();

      cache(1).put(KEY_1, VALUE_2);
      cache(0).put(KEY_2, VALUE_2);
      cache(1).put(KEY_3, VALUE_2);

      assertCachesValue(1, KEY_1, VALUE_2);
      assertCachesValue(0, KEY_2, VALUE_2);
      assertCachesValue(1, KEY_3, VALUE_2);

      tm0.resume(tx0);
      assert VALUE_1.equals(cache(0).get(KEY_1));
      assert VALUE_1.equals(cache(0).get(KEY_2));
      assert VALUE_1.equals(cache(0).get(KEY_3));
      tm0.commit();

      assertNoTransactions();
      printDataContainer();
   }

   public void testPrematureAbort() throws Exception {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2);

      put(0, KEY_1, VALUE_1, null);
      put(1, KEY_2, VALUE_1, null);


      tm(0).begin();
      Object value = cache(0).get(KEY_1);
      assertEquals(VALUE_1, value);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      txPut(1, KEY_1, VALUE_2, VALUE_1);
      txPut(1, KEY_2, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      txPut(0, KEY_1, VALUE_3, VALUE_1);
      try{
         cache(0).get(KEY_2);
         assert false : "Expected to abort read write transaction";
      } catch (Exception e) {
         safeRollback(0);
      }

      printDataContainer();
      assertNoTransactions();
   }

   public void testConflictingTxs() throws Exception {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2);

      put(0, KEY_1, VALUE_1, null);
      put(1, KEY_2, VALUE_1, null);


      tm(0).begin();
      Object value = cache(0).get(KEY_1);
      assertEquals(VALUE_1, value);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      txPut(1, KEY_1, VALUE_2, VALUE_1);
      txPut(1, KEY_2, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      try{
         txPut(0, KEY_1, VALUE_3, VALUE_1);
         txPut(0, KEY_2, VALUE_3, VALUE_1);
         tm(0).commit();
         assert false : "Expected to abort conflicting transaction";
      } catch (Exception e) {
         safeRollback(0);
      }

      printDataContainer();
      assertNoTransactions();
   }

   public void testConcurrentWritesAndReads() throws InterruptedException {
      assertAtLeastCaches(2);
      assertCacheValuesNull(COUNTER_1, COUNTER_2);
      put(0, COUNTER_1, 0, null);
      put(0, COUNTER_2, 0, null);

      ReadWriteThread[] readWriteThreads = new ReadWriteThread[2];
      readWriteThreads[0] = new ReadWriteThread("Writer-0", cache(0));
      readWriteThreads[1] = new ReadWriteThread("Writer-1", cache(1));

      ReadOnlyThread[] readOnlyThreads = new ReadOnlyThread[2];
      readOnlyThreads[0] = new ReadOnlyThread("Reader-0", cache(0));
      readOnlyThreads[1] = new ReadOnlyThread("Reader-1", cache(1));

      for (ReadOnlyThread readOnlyThread : readOnlyThreads) {
         readOnlyThread.start();
      }

      for (ReadWriteThread readWriteThread : readWriteThreads) {
         readWriteThread.start();
      }

      for (ReadWriteThread readWriteThread : readWriteThreads) {
         readWriteThread.join();
      }

      for (ReadOnlyThread readOnlyThread : readOnlyThreads) {
         readOnlyThread.join();
      }

      for (ReadWriteThread readWriteThread : readWriteThreads) {
         assert !readWriteThread.error : "Error occurred in " + readWriteThread.getName();
      }

      for (ReadOnlyThread readOnlyThread : readOnlyThreads) {
         assert !readOnlyThread.error : "Error occurred in " + readOnlyThread.getName();
      }

      printDataContainer();
      assertNoTransactions();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      //no-op
   }

   @Override
   protected int initialClusterSize() {
      return 2;
   }

   @Override
   protected boolean syncCommitPhase() {
      return true;
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.REPL_SYNC;
   }

   private static boolean isEquals(Integer c1, Integer c2) {
      return c1 == null ? c2 == null : c2.equals(c2);
   }

   private class ReadOnlyThread extends Thread {

      private final Cache cache;
      private final TransactionManager transactionManager;
      private boolean run;
      private boolean error;

      private ReadOnlyThread(String threadName, Cache cache) {
         super(threadName);
         this.cache = cache;
         this.transactionManager = cache.getAdvancedCache().getTransactionManager();
         this.run = true;
         this.error = false;
      }

      @Override
      public void run() {
         while (run) {
            if (!consistentRead()) {
               error = false;
               break;
            }
         }
      }

      @Override
      public void interrupt() {
         run = false;
         super.interrupt();
      }

      private boolean consistentRead() {
         try {
            transactionManager.begin();
            Integer c1 = (Integer) cache.get(COUNTER_1);
            Integer c2 = (Integer) cache.get(COUNTER_2);
            transactionManager.commit();
            return isEquals(c1, c2);
         } catch (Exception e) {
            return false;
         }
      }
   }

   private class ReadWriteThread extends Thread {

      private final Cache cache;
      private final TransactionManager transactionManager;
      private boolean run;
      private boolean error;

      private ReadWriteThread(String threadName, Cache cache) {
         super(threadName);
         this.cache = cache;
         this.transactionManager = cache.getAdvancedCache().getTransactionManager();
         this.run = true;
         this.error = false;
      }

      @Override
      public void run() {
         while (run && !error) {
            if (!consistentIncrement()) {
               error = false;
               break;
            }
         }
      }

      @SuppressWarnings("unchecked")
      private boolean consistentIncrement() {
         try {
            transactionManager.begin();
            Integer c1 = (Integer) cache.get(COUNTER_1);
            Integer c2 = (Integer) cache.get(COUNTER_2);

            if (!isEquals(c1, c2)) {
               error = false;
               transactionManager.rollback();
               return false;
            }

            if (c1 == null) {
               c1 = 1;
               c2 = 1;
            } if (c1.equals(1000)) {
               run = false;
               transactionManager.commit();
               return true;
            } else {
               c1++;
               c2++;
            }

            cache.put(COUNTER_1, c1);
            cache.put(COUNTER_2, c2);
            transactionManager.commit();
         } catch (Exception e) {
            safeRollback(transactionManager);
         }
         return true;
      }
   }
}
