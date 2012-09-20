package org.infinispan.dataplacement;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.dataplacement.c50.C50MLObjectLookupFactory;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.test.AbstractCacheTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.C50MLTest")
public class C50MLTest extends AbstractCacheTest{

   private static final String C_50_ML_LOCATION = "/tmp/ml";
   private static final String BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY = "0.001";
   private static final boolean SKIP_ML_RUNNING = true;

   private C50MLObjectLookupFactory objectLookupFactory;

   @BeforeClass
   public void setup() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.dataPlacement()
            .addProperty(C50MLObjectLookupFactory.KEY_FEATURE_MANAGER, DummyKeyFeatureManager.class.getCanonicalName())
            .addProperty(C50MLObjectLookupFactory.LOCATION, C_50_ML_LOCATION)
            .addProperty(C50MLObjectLookupFactory.BF_FALSE_POSITIVE, BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY);
      objectLookupFactory = new C50MLObjectLookupFactory();
      objectLookupFactory.setConfiguration(configurationBuilder.build());
   }

   public void testMachineLearner() {
      if (SKIP_ML_RUNNING) {
         return;
      }

      Map<Object, Integer> movingKeys = new HashMap<Object, Integer>();
      movingKeys.put("1", 1);
      movingKeys.put("2_2", 2);
      movingKeys.put("2_3", 2);
      movingKeys.put("3_12344", 3);
      movingKeys.put("12", 1);
      movingKeys.put("4_4", 4);
      movingKeys.put("5_4", 4);

      ObjectLookup objectLookup = objectLookupFactory.createObjectLookup(movingKeys);

      checkOwnerIndex("1", objectLookup, 1);
      checkOwnerIndex("2_2", objectLookup, 2);
      checkOwnerIndex("2_3", objectLookup, 2);
      checkOwnerIndex("3_12344", objectLookup, 3);
      checkOwnerIndex("12", objectLookup, 1);
      checkOwnerIndex("4_4", objectLookup, 4);
      checkOwnerIndex("5_4", objectLookup, 4);
   }

   private void checkOwnerIndex(Object key, ObjectLookup objectLookup, int expectedOwnerIndex) {
      int ownerIndex = objectLookup.query(key);
      if (ownerIndex != expectedOwnerIndex) {
         log.warnf("Error in Machine Learner + Bloom Filter technique for key " + key + ". Expected owner index is " +
                         expectedOwnerIndex + " returned owner index is " + ownerIndex);
      }
      assert ownerIndex != -1 : "KEY_NOT_FOUND is not possible";
   }
}
