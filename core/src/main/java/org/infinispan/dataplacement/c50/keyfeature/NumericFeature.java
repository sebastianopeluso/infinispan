package org.infinispan.dataplacement.c50.keyfeature;

import java.text.NumberFormat;
import java.text.ParseException;

import static java.lang.Double.compare;

/**
 * Implements a Feature that has as values a number
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NumericFeature implements Feature {

   private static final String[] CLASSES = new String[] {"continuous"};

   private final String name;

   public NumericFeature(String name) {
      this.name = name;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String[] getMachineLearnerClasses() {
      return CLASSES;
   }

   @Override
   public FeatureValueV2 createFeatureValue(Object value) {
      if (value instanceof Number) {
         return new NumericValue((Number) value);
      }
      throw new IllegalArgumentException("Expected a number type value");
   }

   @Override
   public FeatureValueV2 featureValueFromParser(String value) {
      try {
         Number number = NumberFormat.getNumberInstance().parse(value);
         return new NumericValue(number);
      } catch (ParseException e) {
         throw new IllegalStateException("Error parsing value from decision tree");
      }
   }

   public static class NumericValue implements FeatureValueV2 {

      private final Number value;

      private NumericValue(Number value) {
         this.value = value;
      }

      @Override
      public boolean isLessOrEqualsThan(FeatureValueV2 other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) <= 0;
      }

      @Override
      public boolean isGreaterThan(FeatureValueV2 other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) > 0;
      }

      @Override
      public boolean isEquals(FeatureValueV2 other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) == 0;
      }

      @Override
      public String getValueAsString() {
         return value.toString();
      }
   }
}
