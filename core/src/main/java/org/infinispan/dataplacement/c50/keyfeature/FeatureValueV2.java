package org.infinispan.dataplacement.c50.keyfeature;

import java.io.Serializable;

/**
 * Represents an interface for the key feature values
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface FeatureValueV2 extends Serializable {

   /**
    * returns true if this is less or equals than {@code other}. Returns false otherwise or if the {@code other}
    * is not comparable
    *
    * @param other   the other value
    * @return        true if this is less or equals than {@code other}. Returns false otherwise or if the {@code other} 
    *                is not comparable
    */
   boolean isLessOrEqualsThan(FeatureValueV2 other);

   /**
    * returns true if this is greater than {@code other}. Returns false otherwise or if the {@code other}
    * is not comparable
    *
    *
    * @param other   the other value
    * @return        true if this is greater than {@code other}. Returns false otherwise or if the {@code other} 
    *                is not comparable
    */
   boolean isGreaterThan(FeatureValueV2 other);

   /**
    * returns true if this is equals to {@code other}. Returns false otherwise or if the {@code other} is not comparable
    *
    * @param other   the other value
    * @return        true if this is equals to {@code other}. Returns false otherwise or if the {@code other} 
    *                is not comparable
    */
   boolean isEquals(FeatureValueV2 other);

   /**
    * Returns the String representation of the value to write in the .data Machine Learner file
    * 
    * @return  the String representation of the value
    */
   String getValueAsString();

}
