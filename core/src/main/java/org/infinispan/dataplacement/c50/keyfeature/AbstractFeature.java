package org.infinispan.dataplacement.c50.keyfeature;

import java.io.Serializable;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;

/**
 * It represents a key feature
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractFeature implements Serializable {

   public static enum FeatureType {
      NUMBER("continuous."),
      LIST_OF_NAMES(null);

      //the type to put in file.names of C5.0
      //eg continuous. in an age attribute
      final String namesFileValue;

      private FeatureType(String namesFileValue) {
         this.namesFileValue = namesFileValue;
      }

      public String getNamesFileValue() {
         return namesFileValue;
      }
   }

   private FeatureType featureType;
   private String name;

   public AbstractFeature() {} //To serialize / de-serialize

   protected AbstractFeature(String name, FeatureType featureType) {
      this.featureType = featureType;
      this.name = name.replaceAll("\\s", "");
   }

   /**
    * returns the feature type
    * @return  the feature type
    */
   public final FeatureType getType() {
      return featureType;
   }

   /**
    * creates the correct feature value type from the object. It is to be used by the applicatin
    *
    * @param value   the value
    * @return        the feature value
    */
   public final FeatureValue createFeatureValue(Object value) {
      FeatureValue retVal;
      if (featureType == FeatureType.NUMBER) {
         if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Expected a Number but it is " + value.getClass().getSimpleName());
         }
         retVal = new NumericFeatureValue((Number) value);
      } else {
         retVal = new StringFeatureValue(value.toString());
      }
      return retVal;
   }

   /**
    * creates the correct feature value type from a string (e.g. used when reading the rules from the machine
    * learner)
    *
    * @param value   the string with the value
    * @return        the feature value
    */
   public final FeatureValue createFeatureValueFromString(String value) {
      if ("N/A".equals(value)) {
         return null;
      }
      FeatureValue retVal;
      if (featureType == FeatureType.NUMBER) {
         try {
            retVal = new NumericFeatureValue(NumberFormat.getNumberInstance().parse(value));
         } catch (ParseException e) {
            retVal = new NumericFeatureValue(null);
         }
      } else {
         retVal = new StringFeatureValue(value);
      }
      return retVal;
   }

   /**
    * returns the list of possible values for this feature
    *
    * @return  the list of possible values for this feature
    */
   public abstract List<String> getListOfNames();

   /**
    * returns the feature name
    * @return  the feature name
    */
   public final String getName() {
      return name;
   }
}
