package org.infinispan.dataplacement;

import org.infinispan.dataplacement.c50.keyfeature.AbstractFeature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.keyfeature.KeyFeatureManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DummyKeyFeatureManager implements KeyFeatureManager{
   private final AbstractFeature[] features = new AbstractFeature[] {
         new TestFeature("B"),
         new TestFeature("C")
   };

   @Override
   public AbstractFeature[] getAllKeyFeatures() {
      return features;
   }

   @Override
   public Map<AbstractFeature, FeatureValue> getFeatures(Object key) {

      if (!(key instanceof String)) {
         return Collections.emptyMap();
      }

      String[] split = ((String) key).split("_");

      if (split.length == 2) {
         int b = Integer.parseInt(split[0]);
         int c = Integer.parseInt(split[1]);
         Map<AbstractFeature, FeatureValue> features = new HashMap<AbstractFeature, FeatureValue>();
         features.put(this.features[0], this.features[0].createFeatureValue(b));
         features.put(this.features[1], this.features[1].createFeatureValue(c));
         return features;
      } else if (split.length == 1) {
         int c = Integer.parseInt(split[0]);
         Map<AbstractFeature, FeatureValue> features = new HashMap<AbstractFeature, FeatureValue>();
         features.put(this.features[1], this.features[1].createFeatureValue(c));
         return features;
      }

      return Collections.emptyMap();
   }

   private class TestFeature extends AbstractFeature {

      protected TestFeature(String name) {
         super(name, FeatureType.NUMBER);
      }

      @Override
      public List<String> getListOfNames() {
         return null;
      }
   }
}
