package org.infinispan.dataplacement.c50.lookup;

import org.infinispan.dataplacement.c50.keyfeature.AbstractFeature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Represents an element (i.e. it is a rule) in the tree
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class C50MLTreeElement implements Serializable {

   public static final int NO_LEAF_ELEMENT = -1;

   private int ownerIndex;
   private AbstractFeature feature;
   private FeatureValue featureValue;
   private FeatureValue.Condition condition;
   private final List<C50MLTreeElement> children;

   public C50MLTreeElement(int ownerIndex, AbstractFeature feature, String value, String condition) {
      this.ownerIndex = ownerIndex;
      this.feature = feature;
      featureValue = feature.createFeatureValueFromString(value);
      this.condition = FeatureValue.Condition.fromString(condition);
      children = new LinkedList<C50MLTreeElement>();
   }

   public C50MLTreeElement() {
      ownerIndex = NO_LEAF_ELEMENT;
      children = new LinkedList<C50MLTreeElement>();
   } //to externalize/serialize

   /**
    * returns the owner index. it is only valid if the condition is true
    *
    * @return  the owner index
    */
   public final int getOwnerIndex() {
      return ownerIndex;
   }

   /**
    * returns the children elements (rules). it is valid when the condition is true
    *
    * @return  the children elements
    */
   public final List<C50MLTreeElement> getChildren() {
      return children;
   }

   /**
    * adds a child to this element
    *
    * @param child   the tree element
    */
   public final void addChild(C50MLTreeElement child) {
      children.add(child);
   }

   /**
    * checks if the key features matches with the condition in this element
    *
    * @param keyFeatures   the key features
    * @return              true if the key features matches, false otherwise
    */
   public final boolean hasMatch(Map<AbstractFeature, FeatureValue> keyFeatures) {
      boolean notAvailable = featureValue == null;
      FeatureValue value = keyFeatures.get(feature);
      return (notAvailable && value == null) ||
            (!notAvailable && featureValue.valueMatch(value, condition));
   }

   /**
    * returns true if this is a leaf element in the tree (i.e. contains the new owner index)
    *
    * @return  true if this is a leaf element, false otherwise
    */
   public final boolean isLeaf() {
      return ownerIndex != NO_LEAF_ELEMENT;
   }

   /**
    * pretty prints the machine rules represented in this tree
    *
    * @param level         the level of the rule
    * @param stringBuilder the string builder to concatenate this element (and their children)
    */
   public final void printRules(int level, StringBuilder stringBuilder) {
      for (int i = 0; i < level - 1; ++i) {
         stringBuilder.append("|  ");
      }
      if (level > 0) {
         stringBuilder.append("|--");
      }

      stringBuilder.append(feature.getName())
            .append(" ").append(condition)
            .append(" ").append(featureValue == null ? "N/A" : featureValue.getValue());

      if (ownerIndex != NO_LEAF_ELEMENT) {
         stringBuilder.append(" ==> ").append(ownerIndex);
      }

      stringBuilder.append("\n");
      for (C50MLTreeElement child : children) {
         child.printRules(level + 1, stringBuilder);
      }
   }

   /**
    * sets the current element as leaf with the owner index in argument
    *
    * @param ownerIndex the owner index
    */
   public final void setLeaf(int ownerIndex) {
      this.ownerIndex = ownerIndex;
   }

   @Override
   public String toString() {
      return "C50MLTreeElement{" +
            "ownerIndex=" + ownerIndex +
            ", feature=" + feature +
            ", featureValue=" + featureValue +
            ", condition=" + condition +
            ", children=" + children +
            '}';
   }

}
