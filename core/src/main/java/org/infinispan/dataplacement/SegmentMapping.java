package org.infinispan.dataplacement;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SegmentMapping {

   private final int segmentId;
   private final List<KeyOwners> keyOwnersList;

   public SegmentMapping(int segmentId) {
      this.segmentId = segmentId;
      this.keyOwnersList = new LinkedList<KeyOwners>();
   }

   public final int getSegmentId() {
      return segmentId;
   }

   public final void add(Object key, OwnersInfo info) {
      keyOwnersList.add(new KeyOwners(key, info.getNewOwnersIndexes()));
   }

   public final Iterator<KeyOwners> iterator() {
      return keyOwnersList.iterator();
   }

   public static class KeyOwners {
      private final Object key;
      private final int[] ownerIndexes;

      private KeyOwners(Object key, Collection<Integer> ownerIndexes) {
         this.key = key;
         this.ownerIndexes = new int[ownerIndexes.size()];
         int index = 0;
         for (int i : ownerIndexes) {
            this.ownerIndexes[index] = i;
         }
      }

      public final Object getKey() {
         return key;
      }

      public final int[] getOwnerIndexes() {
         return ownerIndexes;
      }
   }
}
