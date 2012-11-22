package org.infinispan.tx.gmu;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.GMUDataContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.IsolationLevel;

import javax.transaction.SystemException;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractGMUTest extends MultipleCacheManagersTest {

   protected static final String KEY_1 = "key_1";
   protected static final String KEY_2 = "key_2";
   protected static final String KEY_3 = "key_3";

   protected static final String VALUE_1 = "value_1";
   protected static final String VALUE_2 = "value_2";
   protected static final String VALUE_3 = "value_3";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = defaultGMUConfiguration();
      decorate(dcc);
      createCluster(dcc, initialClusterSize());
      waitForClusterToForm();
   }

   protected abstract void decorate(ConfigurationBuilder builder);

   protected abstract int initialClusterSize();

   protected abstract boolean syncCommitPhase();

   protected abstract CacheMode cacheMode();


   protected final void assertCachesValue(int executedOn, Object key, Object value) {
      for (int i = 0; i < cacheManagers.size(); ++i) {
         if (i == executedOn || syncCommitPhase()) {
            assertEquals(value, cache(i).get(key));
         } else {
            assertEventuallyEquals(i, key, value);
         }
      }
   }

   protected final void assertCachesValue(Object key, Object value) {
      for (int i = 0; i < cacheManagers.size(); ++i) {
         assertEquals(value, cache(i).get(key));
      }
   }

   protected final void assertCacheValuesNull(Object... keys) {
      for (int i = 0; i < cacheManagers.size(); ++i) {
         for (Object key : keys) {
            assertNull(cache(i).get(key));
         }
      }
   }

   protected final void assertAtLeastCaches(int size) {
      assert cacheManagers.size() >= size;
   }

   protected final void printDataContainer() {
      if (log.isDebugEnabled()) {
         StringBuilder stringBuilder = new StringBuilder("\n\n===================\n");
         for (int i = 0; i < cacheManagers.size(); ++i) {
            DataContainer dataContainer = cache(i).getAdvancedCache().getDataContainer();
            if (dataContainer instanceof GMUDataContainer) {
               stringBuilder.append(dataContainerToString((GMUDataContainer) dataContainer))
                     .append("\n")
                     .append("===================\n");
            } else {
               return;
            }
         }
         log.debugf(stringBuilder.toString());
      }
   }

   protected final void put(int cacheIndex, Object key, Object value, Object returnValue) {
      txPut(cacheIndex, key, value, returnValue);
      assertCachesValue(cacheIndex, key, value);      
   }
   
   protected final void txPut(int cacheIndex, Object key, Object value, Object returnValue) {
      Object oldValue = cache(cacheIndex).put(key, value);      
      assertEquals(returnValue, oldValue);
   }

   protected final void putIfAbsent(int cacheIndex, Object key, Object value, Object returnValue, Object expectedValue) {
      Object oldValue = cache(cacheIndex).putIfAbsent(key, value);
      assertCachesValue(cacheIndex, key, expectedValue);
      assertEquals(returnValue, oldValue);
   }

   protected final void putAll(int cacheIndex, Map<Object, Object> map) {
      cache(cacheIndex).putAll(map);
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
         assertCachesValue(cacheIndex, entry.getKey(), entry.getValue());
      }
   }

   protected final void remove(int cacheIndex, Object key, Object returnValue) {
      Object oldValue = cache(cacheIndex).remove(key);
      assertCachesValue(cacheIndex, key, null);
      assertEquals(returnValue, oldValue);
   }

   protected final void replace(int cacheIndex, Object key, Object value, Object returnValue) {
      Object oldValue = cache(cacheIndex).replace(key, value);
      assertCachesValue(cacheIndex, key, value);
      assertEquals(returnValue, oldValue);
   }

   protected final void replaceIf(int cacheIndex, Object key, Object value, Object ifValue, Object returnValue, boolean success) {
      boolean result = cache(cacheIndex).replace(key, ifValue, value);
      assertCachesValue(cacheIndex, key, returnValue);
      assertEquals(result, success);
   }

   protected final void removeIf(int cacheIndex, Object key, Object ifValue, Object returnValue, boolean success) {
      boolean result = cache(cacheIndex).remove(key, ifValue);
      assertCachesValue(cacheIndex, key, returnValue);
      assertEquals(result, success);
   }
   
   protected final void safeRollback(int cacheIndex) {
      try {
         tm(cacheIndex).rollback();
      } catch (Exception e) {
         log.warn("Exception suppressed when rollback: " + e.getMessage());
      }
   }

   private ConfigurationBuilder defaultGMUConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode(), true);
      builder.locking().isolationLevel(IsolationLevel.SERIALIZABLE);
      builder.versioning().enable().scheme(VersioningScheme.GMU);
      builder.transaction().syncCommitPhase(syncCommitPhase());
      return builder;
   }

   private String dataContainerToString(GMUDataContainer dataContainer) {
      return dataContainer.stateToString();
   }

}
