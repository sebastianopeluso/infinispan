package org.infinispan.dataplacement.c50.tree;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValueV2;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a decision tree in which you can query based on the values of some attributes and returns the new owner
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DecisionTree {

   private final DecisionTreeNode root;

   public DecisionTree(DecisionTreeNode root) {
      this.root = root;
   }

   /**
    * queries the decision tree looking for the value depending of the features value
    *
    * @param keyFeature the feature values
    * @return           the index of the new owner
    */
   public final int query(Map<Feature, FeatureValueV2> keyFeature) {
      DecisionTreeNode node = root.find(keyFeature);
      DecisionTreeNode result = node;

      while (node != null) {
         result = node;
         node = node.find(keyFeature);
      }

      if (result == null) {
         throw new IllegalStateException("Expected to find a decision");
      }

      return result.getValue();
   }

   public static interface DecisionTreeNode extends Serializable {

      /**
       * find and return the best node that respect the feature values
       *
       * @param keyFeatures   the feature values
       * @return              the best node
       */
      DecisionTreeNode find(Map<Feature, FeatureValueV2> keyFeatures);

      /**
       * returns the value of the node (i.e. the new owner)
       *
       * @return  the value of the node (i.e. the new owner)
       */
      int getValue();

   }

}
