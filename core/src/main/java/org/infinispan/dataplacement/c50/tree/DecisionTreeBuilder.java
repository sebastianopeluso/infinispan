package org.infinispan.dataplacement.c50.tree;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;

import java.util.Collection;
import java.util.Map;

import static org.infinispan.dataplacement.c50.tree.DecisionTree.DecisionTreeNode;
import static org.infinispan.dataplacement.c50.tree.ParseTreeNode.EltsValues;

/**
 * Builds a queryable decision tree based on the parsed tree
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DecisionTreeBuilder {

   private final Map<String, Feature> featureMap;

   public DecisionTreeBuilder(Map<String, Feature> featureMap) {
      this.featureMap = featureMap;
   }

   /**
    * returns a queryable decision tree that represents the parsed tree
    *
    * @param root the root node
    * @return     the queryable decision tree
    */
   public final DecisionTree build(ParseTreeNode root) {
      DecisionTreeNode node = parseNode(root);
      return new DecisionTree(node);
   }

   private DecisionTreeNode parseNode(ParseTreeNode node) {
      switch (node.getType()) {
         case 0:
            return parseType0Node(node);
         case 1:
            return parseType1Node(node);
         case 2:
            return parseType2Node(node);
         case 3:
            return parseType3Node(node);
         default:
            throw new IllegalArgumentException("Unkown parsed node type");
      }
   }

   private DecisionTreeNode parseType0Node(ParseTreeNode node) {
      return new Type0Node(getValue(node));
   }

   private DecisionTreeNode parseType1Node(ParseTreeNode node) {
      return new Type1Node(getValue(node), getFeature(node), getForks(node));
   }

   private DecisionTreeNode parseType2Node(ParseTreeNode node) {
      Feature feature = getFeature(node);
      return new Type2Node(getValue(node), feature, getForks(node), getCut(node, feature));
   }

   private DecisionTreeNode parseType3Node(ParseTreeNode node) {
      return new Type3Node(getValue(node), getFeature(node), getForks(node), node.getElts());
   }

   private int getValue(ParseTreeNode node) {
      return Integer.parseInt(node.getClazz());
   }

   private Feature getFeature(ParseTreeNode node) {
      Feature feature = featureMap.get(node.getAttribute());

      if (feature == null) {
         throw new IllegalStateException("Unknown attribute " + node.getAttribute());
      }
      return feature;
   }

   /*
   Decision tree nodes. Each type node has it own find rules and each node has it own attribute (except the leaf node)
    */

   private DecisionTreeNode[] getForks(ParseTreeNode node) {
      DecisionTreeNode[] forks = new DecisionTreeNode[node.getNumberOfForks()];
      ParseTreeNode[] parseForks = node.getForks();

      for (int i = 0; i < forks.length; ++i) {
         forks[i] = parseNode(parseForks[i]);
      }

      return forks;
   }

   private FeatureValue getCut(ParseTreeNode node, Feature feature) {
      String cut = node.getCut();
      return feature.featureValueFromParser(cut);
   }


   private class Type0Node implements DecisionTreeNode {

      private final int value;

      private Type0Node(int value) {
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

   private class Type1Node implements DecisionTreeNode {

      private final int value;
      private final Feature feature;
      private final DecisionTreeNode[] forks;
      private final FeatureValue[] attributeValues;

      private Type1Node(int value, Feature feature, DecisionTreeNode[] forks) {
         this.value = value;
         this.feature = feature;
         if (forks == null || forks.length == 0) {
            throw new IllegalArgumentException("Expected a non-null with at least one fork");
         }

         String[] possibleValues = feature.getMachineLearnerClasses();

         if (forks.length != possibleValues.length + 1) {
            throw new IllegalArgumentException("Number of forks different from the number of possible values");
         }

         this.forks = forks;
         attributeValues = new FeatureValue[forks.length - 1];

         for (int i = 0; i < attributeValues.length; ++i) {
            attributeValues[i] = feature.featureValueFromParser(possibleValues[i]);
         }
      }

      @Override
      public DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures) {
         FeatureValue keyValue = keyFeatures.get(feature);
         if (keyValue == null) { //N/A
            return forks[0];
         }

         for (int i = 0; i < attributeValues.length; ++i) {
            if (attributeValues[i].isEquals(keyValue)) {
               return forks[i + 1];
            }
         }

         throw new IllegalStateException("Expected one value match");
      }

      @Override
      public int getValue() {
         return value;
      }
   }

   private class Type2Node implements DecisionTreeNode {

      private final int value;
      private final Feature feature;
      private final DecisionTreeNode[] forks;
      private final FeatureValue cut;

      private Type2Node(int value, Feature feature, DecisionTreeNode[] forks, FeatureValue cut) {
         this.value = value;
         this.feature = feature;
         this.cut = cut;
         if (forks == null || forks.length == 0) {
            throw new IllegalArgumentException("Expected a non-null with at least one fork");
         }

         if (forks.length != 3) {
            throw new IllegalArgumentException("Expected 3 forks");
         }

         this.forks = forks;
      }

      @Override
      public DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures) {
         FeatureValue keyValue = keyFeatures.get(feature);
         if (keyValue == null) { //N/A
            return forks[0];
         }

         if (keyValue.isLessOrEqualsThan(cut)) {
            return forks[1];
         } else if (keyValue.isGreaterThan(cut)) {
            return forks[2];
         }

         throw new IllegalStateException("Expected one value match");
      }

      @Override
      public int getValue() {
         return value;
      }
   }

   private class Type3Node implements DecisionTreeNode {

      private final int value;
      private final Feature feature;
      private final DecisionTreeNode[] forks;
      private final InternalEltsValues[] values;

      private Type3Node(int value, Feature feature, DecisionTreeNode[] forks, EltsValues[] eltsValues) {
         this.value = value;
         this.feature = feature;

         if (forks == null || forks.length == 0) {
            throw new IllegalArgumentException("Expected a non-null with at least one fork");
         }

         if (forks.length != eltsValues.length) {
            throw new IllegalArgumentException("Expected same number of forks as options");
         }

         this.forks = forks;
         values = new InternalEltsValues[eltsValues.length - 1];

         //the first value is the N/A that it is always in the forks[0]
         for (int i = 0; i < values.length; ++i) {
            values[i] = new InternalEltsValues(eltsValues[i + 1].getValues(), feature);
         }
      }

      @Override
      public DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures) {
         FeatureValue keyValue = keyFeatures.get(feature);
         if (keyValue == null) { //N/A
            return forks[0];
         }

         for (int i = 0; i < values.length; ++i) {
            if (values[i].match(keyValue)) {
               return forks[i + 1];
            }
         }

         throw new IllegalStateException("Expected one value match");
      }

      @Override
      public int getValue() {
         return value;
      }
   }

   private class InternalEltsValues {
      private final FeatureValue[] values;

      private InternalEltsValues(Collection<String> eltsValues, Feature feature) {
         values = new FeatureValue[eltsValues.size()];
         int index = 0;
         for (String value : eltsValues) {
            values[index++] = feature.featureValueFromParser(value);
         }
      }

      private boolean match(FeatureValue keyValue) {
         for (FeatureValue value : values) {
            if (value.isEquals(keyValue)) {
               return true;
            }
         }
         return false;
      }
   }
}
