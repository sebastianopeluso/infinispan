package org.infinispan.tx.gmu.l1;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.gmu.GMUL1Manager;
import org.infinispan.distribution.L1Manager;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.l1.SimpleTest")
public class SimpleTest extends AbstractGMUL1Test {

   public void testEmptyL1Manager() {
      for (int i = 0; i < cacheManagers.size(); ++i) {
         L1Manager l1Manager = getComponent(i, L1Manager.class);
         assert l1Manager instanceof GMUL1Manager : "Unexpected L1 Manager implementation: " + l1Manager.getClass();
      }
   }

   public void testPut() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      Object cache1Key2 = newKey(1, "key_2");
      Object cache1Key3 = newKey(1, "key_3");

      assertCacheValuesNull(cache1Key1, cache1Key2, cache1Key3);

      put(0, cache1Key1, VALUE_1, null);
      assertInL1Cache(0, cache1Key1);

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(cache1Key2, VALUE_2);
      map.put(cache1Key3, VALUE_3);

      putAll(0, map);
      assertInL1Cache(0, cache1Key1, cache1Key2, cache1Key3);

      assertNoTransactions();
      printDataContainer();
      printL1Container();
   }

   public void testPut2() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      assertCacheValuesNull(cache1Key1);

      put(0, cache1Key1, VALUE_1, null);
      assertInL1Cache(0, cache1Key1);

      put(0, cache1Key1, VALUE_2, VALUE_1);
      assertInL1Cache(0, cache1Key1);

      put(0, cache1Key1, VALUE_3, VALUE_2);
      assertInL1Cache(0, cache1Key1);

      put(0, cache1Key1, VALUE_3, VALUE_3);
      assertInL1Cache(0, cache1Key1);

      assertNoTransactions();
      printDataContainer();
      printL1Container();
   }

   public void removeTest() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      assertCacheValuesNull(cache1Key1);

      put(1, cache1Key1, VALUE_1, null);
      remove(0, cache1Key1, VALUE_1);
      assertInL1Cache(0, cache1Key1);

      put(0, cache1Key1, VALUE_2, null);
      assertInL1Cache(0, cache1Key1);

      remove(1, cache1Key1, VALUE_2);
      assertInL1Cache(0, cache1Key1);

      assertNoTransactions();
      printDataContainer();
      printL1Container();
   }

   public void testPutIfAbsent() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      Object cache0Key2 = newKey(0, "key_2");
      assertCacheValuesNull(cache1Key1, KEY_2);

      put(1, cache0Key2, VALUE_1, null);
      assertInL1Cache(1, cache0Key2);

      putIfAbsent(0, cache0Key2, VALUE_2, VALUE_1, VALUE_1);
      assertInL1Cache(0, cache1Key1);
      assertInL1Cache(1, cache0Key2);

      put(1, cache0Key2, VALUE_2, VALUE_1);
      assertInL1Cache(1, cache0Key2);
      assertInL1Cache(0, cache1Key1);

      putIfAbsent(0, cache0Key2, VALUE_3, VALUE_2, VALUE_2);
      assertInL1Cache(0, cache1Key1);
      assertInL1Cache(1, cache0Key2);

      putIfAbsent(0, cache1Key1, VALUE_3, null, VALUE_3);
      assertInL1Cache(0, cache1Key1);
      assertInL1Cache(1, cache0Key2);


      assertNoTransactions();
      printDataContainer();
      printL1Container();
   }

   public void testRemoveIfPresent() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      assertCacheValuesNull(KEY_1);

      put(0, cache1Key1, VALUE_1, null);
      assertInL1Cache(0, cache1Key1);

      put(1, cache1Key1, VALUE_2, VALUE_1);
      assertInL1Cache(0, cache1Key1);

      removeIf(0, cache1Key1, VALUE_1, VALUE_2, false);
      assertInL1Cache(0, cache1Key1);

      removeIf(0, cache1Key1, VALUE_2, null, true);
      assertInL1Cache(0, cache1Key1);

      assertNoTransactions();
      printDataContainer();
      printL1Container();
   }

   public void testClear() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      assertCacheValuesNull(cache1Key1);

      put(0, cache1Key1, VALUE_1, null);
      assertInL1Cache(0, cache1Key1);

      cache(0).clear();
      assertCachesValue(0, cache1Key1, null);

      assertNoTransactions();
      printDataContainer();
      printL1Container();
   }

   public void testReplace() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      assertCacheValuesNull(cache1Key1);

      put(1, cache1Key1, VALUE_1, null);
      replace(0, cache1Key1, VALUE_2, VALUE_1);
      assertInL1Cache(0, cache1Key1);

      put(0, cache1Key1, VALUE_3, VALUE_2);
      assertInL1Cache(0, cache1Key1);

      replace(0, cache1Key1, VALUE_3, VALUE_3);
      assertInL1Cache(0, cache1Key1);

      assertNoTransactions();
      printDataContainer();
      printL1Container();
   }

   public void testReplaceWithOldVal() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      assertCacheValuesNull(KEY_1);

      put(1, cache1Key1, VALUE_2, null);
      put(0, cache1Key1, VALUE_3, VALUE_2);
      assertInL1Cache(0, cache1Key1);

      replaceIf(0, cache1Key1, VALUE_1, VALUE_2, VALUE_3, false);
      assertInL1Cache(0, cache1Key1);

      replaceIf(0, cache1Key1, VALUE_1, VALUE_3, VALUE_1, true);
      assertInL1Cache(0, cache1Key1);

      assertNoTransactions();
      printDataContainer();
      printL1Container();   }

   public void testRemoveUnexistingEntry() {
      assertAtLeastCaches(2);
      Object cache1Key1 = newKey(1, "key_1");
      assertCacheValuesNull(cache1Key1);

      remove(0, cache1Key1, null);
      assertInL1Cache(0, cache1Key1);

      assertNoTransactions();
      printDataContainer();
      printL1Container();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
   }

   @Override
   protected int initialClusterSize() {
      return 2;
   }

   @Override
   protected boolean syncCommitPhase() {
      return true;
   }
}
