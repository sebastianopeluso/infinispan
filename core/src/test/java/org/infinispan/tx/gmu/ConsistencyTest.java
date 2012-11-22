package org.infinispan.tx.gmu;

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
}
