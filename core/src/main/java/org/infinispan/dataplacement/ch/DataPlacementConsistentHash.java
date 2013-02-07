package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConsistentHash<CH extends ConsistentHash> implements ConsistentHash {

   private final CH consistentHash;
   //one object lookup per segment
   private final ObjectLookup[] objectLookups;
   //the cluster snapshot when the object lookups was created
   private final ClusterSnapshot clusterSnapshot;

   public DataPlacementConsistentHash(CH consistentHash, ObjectLookup[] objectLookups, ClusterSnapshot clusterSnapshot) {
      this.consistentHash = consistentHash;
      this.objectLookups = objectLookups;
      this.clusterSnapshot = clusterSnapshot;
   }

   public DataPlacementConsistentHash(DataPlacementConsistentHash<CH> baseCH, CH consistentHash) {
      this(consistentHash, baseCH.objectLookups, baseCH.clusterSnapshot);
   }

   @Override
   public int getNumOwners() {
      return consistentHash.getNumOwners();
   }

   @Override
   public Hash getHashFunction() {
      return consistentHash.getHashFunction();
   }

   @Override
   public int getNumSegments() {
      return consistentHash.getNumSegments();
   }

   @Override
   public List<Address> getMembers() {
      return consistentHash.getMembers();
   }

   @Override
   public Address locatePrimaryOwner(Object key) {
      List<Address> newOwners = getNewOwnersForKey(key, 1);
      return newOwners == null || newOwners.isEmpty() ? consistentHash.locatePrimaryOwner(key) :
            newOwners.get(0);
   }

   @Override
   public List<Address> locateOwners(Object key) {      
      List<Address> newOwners = getNewOwnersForKey(key, consistentHash.getNumOwners());
      List<Address> defaultOwners = consistentHash.locateOwners(key);      
      
      return newOwners == null || newOwners.isEmpty() ? defaultOwners :
            merge(defaultOwners, newOwners, consistentHash.getNumOwners());
   }

   @Override
   public Set<Address> locateAllOwners(Collection<Object> keys) {
      Set<Address> owners = new HashSet<Address>();
      for (Object key : keys) {
         owners.addAll(locateOwners(key));
      }
      return owners;
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return locateOwners(key).contains(nodeAddress);
   }

   @Override
   public int getSegment(Object key) {
      return consistentHash.getSegment(key);
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return consistentHash.locateOwnersForSegment(segmentId);
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return consistentHash.locatePrimaryOwnerForSegment(segmentId);
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      return consistentHash.getSegmentsForOwner(owner);
   }

   public final CH getConsistentHash() {
      return consistentHash;
   }

   private List<Address> merge(List<Address> defaultOwners, List<Address> newOwners, int numberOfOwners) {
      List<Address> merged = new ArrayList<Address>(numberOfOwners);
      //first put owners that are in the default and new owners list
      for (Iterator<Address> iterator = defaultOwners.iterator(); iterator.hasNext() && merged.size() < numberOfOwners; ) {
         Address defaultOwner = iterator.next();
         int index = newOwners.indexOf(defaultOwner);
         if (index >= 0) {
            merged.add(defaultOwner);
            newOwners.remove(index);
            iterator.remove();
         }
      }

      //then add the remaining new owners list (if needed)
      for (Iterator<Address> iterator = newOwners.iterator(); iterator.hasNext() && merged.size() < numberOfOwners; ) {
         merged.add(iterator.next());
      }

      //finally the default owners list (if needed)
      for (Iterator<Address> iterator = defaultOwners.iterator(); iterator.hasNext() && merged.size() < numberOfOwners; ) {
         merged.add(iterator.next());
      }
      return merged;
   }

   private List<Address> getNewOwnersForKey(Object key, int numberOfOwners) {
      List<Integer> newOwnersIndexes = objectLookups[consistentHash.getSegment(key)].query(key);
      if (newOwnersIndexes == null || newOwnersIndexes.isEmpty()) {
         return null;
      }
      List<Address> newOwners = new ArrayList<Address>(Math.min(numberOfOwners, newOwnersIndexes.size()));
      for (Iterator<Integer> iterator = newOwnersIndexes.iterator(); iterator.hasNext() && newOwners.size() < numberOfOwners; ) {
         Address owner = clusterSnapshot.get(iterator.next());
         if (owner != null && consistentHash.getMembers().contains(owner)) {
            newOwners.add(owner);
         }
      }
      return newOwners;
   }
}
