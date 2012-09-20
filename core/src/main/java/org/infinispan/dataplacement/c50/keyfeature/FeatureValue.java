package org.infinispan.dataplacement.c50.keyfeature;

import java.io.Serializable;

/**
 * Represents a value from a key feature
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface FeatureValue extends Serializable {

   //the possible conditions for the ML C5.0
   public static enum Condition {
      EQ,
      LE,
      GT;

      public static Condition fromString(String condition) {
         if (condition.equals("=")) {
            return EQ;
         } else if (condition.equals("<=")) {
            return LE;
         } else if (condition.equals(">")) {
            return GT;
         }
         return null;
      }
   }

   /**
    * returns true if the parameter passed matches with this value, depending of the condition
    *
    * Note:
    *    this compares the otherValue with this value. eg. 1.valueMatch(2, LE) returns false!
    *
    * @param otherValue the value
    * @param condition  the condition
    * @return           true if the value matches this value, false otherwise
    */
   boolean valueMatch(FeatureValue otherValue, Condition condition);

   /**
    * returns the value
    * 
    * @return  the value
    */
   Object getValue();

}
