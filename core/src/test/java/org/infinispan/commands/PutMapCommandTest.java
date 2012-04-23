package org.infinispan.commands;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
@Test(groups = "functional", testName = "commands.PutMapCommandTest")
public class PutMapCommandTest extends MultipleCacheManagersTest {
   protected int numberOfKeys = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1);
      dcc.locking().transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createCluster(dcc, 4);
      waitForClusterToForm();
   }

   public void testPutMapCommand() {
      for (int i = 0; i < numberOfKeys; ++i) {
         assert cache(0).get("key" + i) == null;
         assert cache(1).get("key" + i) == null;
         assert cache(2).get("key" + i) == null;
         assert cache(3).get("key" + i) == null;
      }

      Map<String, String> map = new HashMap<String, String>();
      for (int i = 0; i < numberOfKeys; ++i) {
         map.put("key" + i, "value" + i);
      }

      cache(0).putAll(map);

      for (int i = 0; i < numberOfKeys; ++i) {
         assert cache(0).get("key" + i).equals("value" + i);
         final int finalI = i;
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(1).get("key" + finalI).equals("value" + finalI);
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(2).get("key" + finalI).equals("value" + finalI);
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return cache(3).get("key" + finalI).equals("value" + finalI);
            }
         });
      }
   }
}
