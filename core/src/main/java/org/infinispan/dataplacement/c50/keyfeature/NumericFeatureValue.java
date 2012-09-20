package org.infinispan.dataplacement.c50.keyfeature;

/**
 * A numeric feature value
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NumericFeatureValue implements FeatureValue {

   //null means N/A or NaN
   private final Number value;

   public NumericFeatureValue(Number value) {
      this.value = value;
   }

   @Override
   public boolean valueMatch(FeatureValue otherValue, Condition condition) {

      if (otherValue == null) {
         return value == null;
      } else if (value == null) {
         return false;
      } else if (!(otherValue instanceof NumericFeatureValue)) {
         return false;
      }

      NumericFeatureValue numericFeatureValue = (NumericFeatureValue) otherValue;
      
      if (numericFeatureValue.value == null) {
         return false;
      }
      
      int compare = Double.compare(numericFeatureValue.value.doubleValue(), this.value.doubleValue());

      switch (condition) {
         case EQ:
            return compare == 0;
         case LE:
            return compare <= 0;
         case GT:
            return compare > 0;
      }

      return false;
   }

   @Override
   public Object getValue() {
      return value;
   }
}
