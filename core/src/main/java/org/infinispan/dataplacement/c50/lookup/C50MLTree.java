package org.infinispan.dataplacement.c50.lookup;

import org.infinispan.dataplacement.c50.keyfeature.AbstractFeature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.lookup.ObjectLookup;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * A tree that represents the Machine Learner rules and can be query for the owner of some key
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class C50MLTree implements Serializable {

   private final C50MLTreeElement rootElement;

   public C50MLTree(C50MLTreeElement rootElement) {
      this.rootElement = rootElement;
   }

   /**
    * tries to find the key in the tree and the new owner
    *
    * @param keysFeatures  the key features
    * @return              the new owner index or KEY_NOT_FOUND if the keys was not moved
    */
   public final int getOwnerIndex(Map<AbstractFeature, FeatureValue> keysFeatures) {
      if (keysFeatures == null || keysFeatures.isEmpty()) {
         return ObjectLookup.KEY_NOT_FOUND;
      }

      C50MLTreeElement currentElement = rootElement;
      while (currentElement != null) {
         if (currentElement.isLeaf()) {
            return currentElement.getOwnerIndex();
         }
         currentElement = findElement(currentElement.getChildren(), keysFeatures);
      }
      return ObjectLookup.KEY_NOT_FOUND;
   }

   /**
    * tries to find a tree element that matches with the features values (and conditions)
    *
    * @param children      the list of tree elements
    * @param keyFeatures   the key features
    * @return              the tree element that matches, null otherwise
    */
   private C50MLTreeElement findElement(Collection<C50MLTreeElement> children, Map<AbstractFeature, FeatureValue> keyFeatures) {
      for (C50MLTreeElement child : children) {
         if (child.hasMatch(keyFeatures)) {
            return child;
         }
      }
      return null;
   }

   public final String rulesToString() {
      StringBuilder sb = new StringBuilder("Rules:\n");
      for (C50MLTreeElement element : rootElement.getChildren()) {
         element.printRules(0, sb);
      }
      return sb.toString();
   }
}
