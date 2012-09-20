package org.infinispan.dataplacement;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.dataplacement.c50.C50MLObjectLookupFactory;
import org.infinispan.dataplacement.c50.C50MLRulesParser;
import org.infinispan.dataplacement.c50.lookup.C50MLTree;
import org.infinispan.dataplacement.c50.keyfeature.AbstractFeature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.util.FileLookupFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.C50MLTest")
public class C50MLTest extends AbstractCacheTest{

   private static final String C_50_OUTPUT_EXAMPLE = "./c50output";
   private static final String C_50_ML_LOCATION = "/tmp/ml";
   private static final String BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY = "0.001";
   private static final boolean SKIP_ML_RUNNING = false;

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

   public void testOutputParser() {
      BufferedReader reader = getC50OutputBufferedReader();
      assert reader != null : "example file not found: " + C_50_OUTPUT_EXAMPLE;

      Collection<String> rules = objectLookupFactory.getRulesFromMachineLearner(reader);

      assert rules.size() == 7 : "Expected the rules in 7 lines";

      Iterator<String> rulesIt = rules.iterator();

      assert "B = N/A: 1 (27/3)".equals(rulesIt.next()) : "Wrong first rule";
      assert "B > 2: 2 (121)".equals(rulesIt.next()) : "Wrong second rule";
      assert "B <= 2:".equals(rulesIt.next()) : "Wrong third rule";
      assert ":...C > 2334: 3 (403)".equals(rulesIt.next()) : "Wrong fourth rule";
      assert "    C <= 2334:".equals(rulesIt.next()) : "Wrong fifth rule";
      assert "    :...B <= 1: 4 (130)".equals(rulesIt.next()) : "Wrong sixth rule";
      assert "        B > 1: 5 (8)".equals(rulesIt.next()) : "Wrong seventh rule";
   }

   public void testTreeParser() {
      BufferedReader reader = getC50OutputBufferedReader();
      assert reader != null : "example file not found: " + C_50_OUTPUT_EXAMPLE;

      Collection<String> rules = objectLookupFactory.getRulesFromMachineLearner(reader);

      C50MLTree c50MLTree = new C50MLRulesParser(rules, objectLookupFactory.getFeatureMap()).parseMachineLearnerRules();      

      //b = N/A ==> 1
      assertOwnerIndex("1", c50MLTree, 1);
      assertOwnerIndex("2", c50MLTree, 1);

      //B > 2  ==> 2
      assertOwnerIndex("3_1", c50MLTree, 2);
      assertOwnerIndex("4_4", c50MLTree, 2);

      // B <= 2 && C > 2334 ==> 3
      assertOwnerIndex("2_2335", c50MLTree, 3);
      assertOwnerIndex("1_2335", c50MLTree, 3);

      //B <= 2 && C <= 2334 && B <= 1 ==> 4
      assertOwnerIndex("1_2334", c50MLTree, 4);
      assertOwnerIndex("0_2334", c50MLTree, 4);
      assertOwnerIndex("1_2333", c50MLTree, 4);

      //B <= 2 && C <= 2334 && B > 1 ==> 5
      assertOwnerIndex("2_2334", c50MLTree, 5);
      assertOwnerIndex("2_2333", c50MLTree, 5);
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

   private void assertOwnerIndex(Object key, C50MLTree tree, int expectedOwnerIndex) {
      Map<AbstractFeature, FeatureValue> keyFeatures = objectLookupFactory.getKeyFeatureManager().getFeatures(key);
      int ownerIndex = tree.getOwnerIndex(keyFeatures);
      assert ownerIndex == expectedOwnerIndex : "Expected the owner index of " + expectedOwnerIndex + " for key " +
            key + " but it returned the owner index of " + ownerIndex;
   }

   private void checkOwnerIndex(Object key, ObjectLookup objectLookup, int expectedOwnerIndex) {
      int ownerIndex = objectLookup.query(key);
      if (ownerIndex != expectedOwnerIndex) {
         log.warnf("Error in Machine Learner + Bloom Filter technique for key " + key + ". Expected owner index is " + 
                         expectedOwnerIndex + " returned owner index is " + ownerIndex);
      }
      assert ownerIndex != -1 : "KEY_NOT_FOUND is not possible";
   }

   private BufferedReader getC50OutputBufferedReader() {
      InputStream is = FileLookupFactory.newInstance().lookupFile(C_50_OUTPUT_EXAMPLE,
                                                                  Thread.currentThread().getContextClassLoader());
      if (is == null) {
         return null;
      }
      return new BufferedReader(new InputStreamReader(is));
   }
}
