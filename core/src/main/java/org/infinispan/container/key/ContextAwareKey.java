package org.infinispan.container.key;

/**
 * A Key that is aware about the immutability of the identified values.
 * 
 * @author Sebastiano Peluso
 * @since 5.2
 */
public interface ContextAwareKey {

   /**
    * Returns true if the value associated to this key is immutable.
    *
    * @return true if the value associated to this key is immutable.
    */
   boolean identifyImmutableValue();

}
