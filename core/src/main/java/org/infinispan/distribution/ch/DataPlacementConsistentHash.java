package org.infinispan.distribution.ch;

import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * The consistent hash function implementation that the Object Lookup implementations from the Data Placement 
 * optimization
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConsistentHash extends AbstractConsistentHash {

   private ConsistentHash defaultConsistentHash;
   private final InternalLookup[] internalLookup;
   private final ClusterSnapshot clusterSnapshot;

   public DataPlacementConsistentHash(ClusterSnapshot clusterSnapshot) {
      this.clusterSnapshot = clusterSnapshot;
      internalLookup = new InternalLookup[clusterSnapshot.size()];
   }

   public void addObjectLookup(Address address, Collection<ObjectLookup> objectLookup) {
      if (objectLookup == null) {
         return;
      }
      int index = clusterSnapshot.indexOf(address);
      if (index == -1) {
         return;
      }
      internalLookup[index] = new InternalLookup(objectLookup);
   }

   @Override
   public void setCaches(Set<Address> caches) {
      defaultConsistentHash.setCaches(caches);
   }

   @Override
   public Set<Address> getCaches() {
      return defaultConsistentHash.getCaches();
   }

   @Override
   public List<Address> locate(Object key, int replCount) {
      List<Address> defaultOwners = defaultConsistentHash.locate(key, replCount);
      int primaryOwnerIndex = clusterSnapshot.indexOf(defaultOwners.get(0));

      if (primaryOwnerIndex == -1) {
         return defaultOwners;
      }

      InternalLookup lookup = internalLookup[primaryOwnerIndex];

      if (lookup == null) {
         return defaultOwners;
      }

      List<Address> newOwners = lookup.query(key);

      return newOwners.size() == replCount ? newOwners : defaultOwners;
   }

   @Override
   public List<Integer> getHashIds(Address a) {
      return Collections.emptyList();
   }


   public void setDefault(ConsistentHash defaultHash) {
      defaultConsistentHash = defaultHash;
   }

   public ConsistentHash getDefaultHash() {
      return defaultConsistentHash;
   }

   private class InternalLookup {
      private final Collection<ObjectLookup> objectsLookup;

      private InternalLookup(Collection<ObjectLookup> objectsLookup) {
         this.objectsLookup = objectsLookup;
      }

      public List<Address> query(Object key) {
         List<Address> owners = new LinkedList<Address>();
         for (ObjectLookup lookup : objectsLookup) {
            int owner = lookup.query(key);
            if (owner != -1) {
               owners.add(clusterSnapshot.get(owner));
            }
         }
         return owners;
      }

   }
}
