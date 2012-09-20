package org.infinispan.dataplacement.c50.tree.node;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;

import java.util.Map;

/**
 * Type 0 decision tree node: the leaf node
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class Type0Node implements DecisionTreeNode {

   private final int value;

   public Type0Node(int value) {
      this.value = value;
   }

   @Override
   public DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures) {
      return null; //it is a leaf node
   }

   @Override
   public int getValue() {
      return value;
   }
}
