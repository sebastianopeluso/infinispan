package org.infinispan.dataplacement.c50.tree;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.tree.node.DecisionTreeNode;
import org.infinispan.dataplacement.c50.tree.node.Type0Node;
import org.infinispan.dataplacement.c50.tree.node.Type1Node;
import org.infinispan.dataplacement.c50.tree.node.Type2Node;
import org.infinispan.dataplacement.c50.tree.node.Type3Node;

import java.util.Map;

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
}
