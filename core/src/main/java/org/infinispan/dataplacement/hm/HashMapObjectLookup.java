package org.infinispan.dataplacement.hm;

import org.infinispan.dataplacement.OwnersInfo;
import org.infinispan.dataplacement.lookup.ObjectLookup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * the object lookup implementation for the Hash Map technique
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class HashMapObjectLookup implements ObjectLookup {

   private final Map<Object, List<Integer>> lookup;

   public HashMapObjectLookup(Map<Object, OwnersInfo> keysToMove) {
      lookup = new HashMap<Object, List<Integer>>();

      for (Map.Entry<Object, OwnersInfo> entry : keysToMove.entrySet()) {
         lookup.put(entry.getKey(), entry.getValue().getNewOwnersIndexes());
      }
   }

   @Override
   public List<Integer> query(Object key) {
      return lookup.get(key);
   }
}
