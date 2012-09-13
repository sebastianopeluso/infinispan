package org.infinispan.dataplacement;

import java.util.ArrayList;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OwnersInfo {
   private final ArrayList<Integer> ownersIndexes;
   private final ArrayList<Long> ownersAccesses;

   public OwnersInfo(int size) {
      ownersIndexes = new ArrayList<Integer>(size);
      ownersAccesses = new ArrayList<Long>(size);
   }

   public void add(int ownerIndex, long numberOfAccesses) {
      ownersIndexes.add(ownerIndex);
      ownersAccesses.add(numberOfAccesses);
   }

   public void calculateNewOwner(int requestIdx, long numberOfAccesses) {
      int toReplaceIndex = -1;
      long minAccesses = numberOfAccesses;

      for (int index = 0; index < ownersAccesses.size(); ++index) {
         if (ownersAccesses.get(index) < minAccesses) {
            minAccesses = ownersAccesses.get(index);
            toReplaceIndex = index;
         }
      }

      if (toReplaceIndex != -1) {
         ownersIndexes.set(toReplaceIndex, requestIdx);
         ownersAccesses.set(toReplaceIndex, numberOfAccesses);
      }
   }
}
