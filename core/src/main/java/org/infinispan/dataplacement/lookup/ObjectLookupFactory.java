package org.infinispan.dataplacement.lookup;

import org.infinispan.configuration.cache.Configuration;

import java.util.Collection;
import java.util.Map;

/**
 * Interface that creates the Object Lookup instances based on the keys to be moved
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ObjectLookupFactory {

   /**
    * sets the Infinispan configuration to configure this object lookup factory
    *
    * @param configuration the Infinispan configuration
    */
   void setConfiguration(Configuration configuration);

   /**
    * creates the {@link ObjectLookup} corresponding to the keys to be moved
    *
    * @param keysToMove the keys to move (Object) and the new owner (Integer)
    * @return           the object lookup or null if it is not possible to create it    
    */
   ObjectLookup createObjectLookup(Map<Object, Integer> keysToMove);

   /**
    * Serializes in a array of objects the information to send for other nodes 
    *
    *
    * @param objectLookupCollection a collection of objects lookup to serialize
    * @return                       the array of objects
    */
   Object[] serializeObjectLookup(Collection<ObjectLookup> objectLookupCollection);

   /**
    * Creates the object lookup from the parameters from other nodes
    *
    *
    * @param parameters the parameters
    * @return           the object lookup instance (or null if it is not possible to create it)                          
    */
   Collection<ObjectLookup> deSerializeObjectLookup(Object[] parameters);

}
