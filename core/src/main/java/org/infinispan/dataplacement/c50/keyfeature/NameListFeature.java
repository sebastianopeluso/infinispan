package org.infinispan.dataplacement.c50.keyfeature;

/**
 * Implements a Feature that has as values a list of names
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NameListFeature implements Feature {

   private final String[] classes;
   private final String name;

   public NameListFeature(String name, String... classes) {
      this.name = name;
      if (classes == null || classes.length <= 1) {
         throw new IllegalArgumentException("Expected non-null and more than one classes");
      }
      this.classes = classes;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String[] getMachineLearnerClasses() {
      return classes;
   }

   @Override
   public FeatureValueV2 createFeatureValue(Object value) {
      if (value instanceof String) {
         return new StringValue((String) value);
      }
      throw new IllegalArgumentException("Expected a String type value");
   }

   @Override
   public FeatureValueV2 featureValueFromParser(String value) {
      return new StringValue(value);
   }

   public static class StringValue implements FeatureValueV2 {

      private final String value;

      private StringValue(String value) {
         this.value = value;
      }

      @Override
      public boolean isLessOrEqualsThan(FeatureValueV2 other) {
         return false;
      }

      @Override
      public boolean isGreaterThan(FeatureValueV2 other) {
         return false;
      }

      @Override
      public boolean isEquals(FeatureValueV2 other) {
         return other instanceof StringValue && value.equals(((StringValue) other).value);
      }

      @Override
      public String getValueAsString() {
         return value;
      }
   }
}
