package org.infinispan.dataplacement.c50.keyfeature;

/**
 * A string feature value
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StringFeatureValue implements FeatureValue {

   private final String value;

   public StringFeatureValue(String value) {
      this.value = value.replaceAll("\\s,[.]:[|]", "");
   }

   @Override
   public boolean valueMatch(FeatureValue otherValue, Condition condition) {
      if (otherValue == null) {
         return value == null;
      }

      if (!(otherValue instanceof StringFeatureValue)) {
         return false;
      }

      StringFeatureValue stringFeatureValue = (StringFeatureValue) otherValue;

      switch (condition) {
         case EQ:
            return this.value.equals(stringFeatureValue.value);
      }

      return false;
   }

   @Override
   public Object getValue() {
      return value;
   }
}
