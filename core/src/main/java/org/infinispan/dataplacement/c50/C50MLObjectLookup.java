package org.infinispan.dataplacement.c50;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.keyfeature.KeyFeatureManager;
import org.infinispan.dataplacement.c50.lookup.BloomFilter;
import org.infinispan.dataplacement.c50.tree.DecisionTree;
import org.infinispan.dataplacement.lookup.ObjectLookup;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * the object lookup implementation for the Bloom Filter + Machine Learner technique
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class C50MLObjectLookup implements ObjectLookup {

   private final BloomFilter bloomFilter;
   private final DecisionTree[] decisionTreeArray;
   private transient KeyFeatureManager keyFeatureManager;

   public C50MLObjectLookup(int numberOfOwners, BloomFilter bloomFilter) {
      this.bloomFilter = bloomFilter;
      decisionTreeArray = new DecisionTree[numberOfOwners];
   }

   public void setDecisionTreeList(int index, DecisionTree decisionTree) {
      decisionTreeArray[index] = decisionTree;
   }

   public void setKeyFeatureManager(KeyFeatureManager keyFeatureManager) {
      this.keyFeatureManager = keyFeatureManager;
   }

   @Override
   public List<Integer> query(Object key) {
      if (!bloomFilter.contains(key)) {
         return null;
      } else {
         Map<Feature, FeatureValue> keyFeatures = keyFeatureManager.getFeatures(key);
         List<Integer> owners = new LinkedList<Integer>();

         for (DecisionTree tree : decisionTreeArray) {
            owners.add(tree.query(keyFeatures));
         }
         return owners;
      }
   }
}
