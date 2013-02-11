package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
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
   private final List<ClusterObjectLookup> clusterObjectLookupList;

   public DataPlacementConsistentHash(CH consistentHash, ObjectLookup[] objectLookups, ClusterSnapshot clusterSnapshot) {
      this.consistentHash = consistentHash;
      this.clusterObjectLookupList = new LinkedList<ClusterObjectLookup>();
      if (objectLookups != null) {
         this.clusterObjectLookupList.add(new ClusterObjectLookup(objectLookups, clusterSnapshot));
      }
   }

   public DataPlacementConsistentHash(DataPlacementConsistentHash<CH> baseCH, CH consistentHash) {
      this.consistentHash = consistentHash;
      this.clusterObjectLookupList = new LinkedList<ClusterObjectLookup>(baseCH.clusterObjectLookupList);
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
            merge(defaultOwners, newOwners, defaultOwners.size());
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
      List<Address> newOwners = new LinkedList<Address>();

      for (ClusterObjectLookup clusterObjectLookup : clusterObjectLookupList) {
         mergeUnique(newOwners, clusterObjectLookup.getNewOwnersForKey(key, consistentHash, numberOfOwners));
      }

      return newOwners;
   }

   private void mergeUnique(List<Address> result, List<Address> toMerge) {
      if (toMerge == null) {
         return;
      }
      for (Address address : toMerge) {
         if (!result.contains(address)) {
            result.add(address);
         }
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DataPlacementConsistentHash that = (DataPlacementConsistentHash) o;

      return !(clusterObjectLookupList != null ? !clusterObjectLookupList.equals(that.clusterObjectLookupList) : that.clusterObjectLookupList != null) &&
            !(consistentHash != null ? !consistentHash.equals(that.consistentHash) : that.consistentHash != null);

   }

   @Override
   public int hashCode() {
      int result = consistentHash != null ? consistentHash.hashCode() : 0;
      result = 31 * result + (clusterObjectLookupList != null ? clusterObjectLookupList.hashCode() : 0);
      return result;
   }

   public Object[] toParameters() {
      if (clusterObjectLookupList.isEmpty()) {
         return null;
      }
      Object[] parameters = new Object[clusterObjectLookupList.size() * 2];
      int index = 0;
      for (ClusterObjectLookup clusterObjectLookup : clusterObjectLookupList) {
         parameters[index++] = clusterObjectLookup.objectLookups;
         parameters[index++] = clusterObjectLookup.clusterSnapshot.getMembers();
      }
      return parameters;
   }

   public void fromParameters(Object[] parameters) {
      if (parameters == null) {
         return;
      }
      for (int i = 0; i < parameters.length; ++i) {
         ObjectLookup[] lookups = (ObjectLookup[]) parameters[i++];
         Collection<Address> members = (Collection<Address>) parameters[i];
         clusterObjectLookupList.add(new ClusterObjectLookup(lookups, new ClusterSnapshot(members, consistentHash.getHashFunction())));
      }
   }

   public static class Externalizer extends AbstractExternalizer<DataPlacementConsistentHash> {

      @Override
      public Set<Class<? extends DataPlacementConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends DataPlacementConsistentHash>>asSet(DataPlacementConsistentHash.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DataPlacementConsistentHash object) throws IOException {
         output.writeObject(object.consistentHash);
         output.writeObject(object.toParameters());
      }

      @Override
      public DataPlacementConsistentHash readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ConsistentHash consistentHash = (ConsistentHash) input.readObject();
         Object[] parameters = (Object[]) input.readObject();
         DataPlacementConsistentHash dataPlacementConsistentHash = new DataPlacementConsistentHash(consistentHash, null, null);
         dataPlacementConsistentHash.fromParameters(parameters);
         return dataPlacementConsistentHash;
      }

      @Override
      public Integer getId() {
         return Ids.DATA_PLACEMENT_CONSISTENT_HASH;
      }
   }

   private class ClusterObjectLookup {
      private final ObjectLookup[] objectLookups;
      private final ClusterSnapshot clusterSnapshot;

      private ClusterObjectLookup(ObjectLookup[] objectLookups, ClusterSnapshot clusterSnapshot) {
         this.objectLookups = objectLookups;
         this.clusterSnapshot = clusterSnapshot;
      }

      public final List<Address> getNewOwnersForKey(Object key, ConsistentHash consistentHash, int numberOfOwners) {
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

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         ClusterObjectLookup that = (ClusterObjectLookup) o;

         return !(clusterSnapshot != null ? !clusterSnapshot.equals(that.clusterSnapshot) : that.clusterSnapshot != null) && 
               Arrays.equals(objectLookups, that.objectLookups);

      }

      @Override
      public int hashCode() {
         int result = objectLookups != null ? Arrays.hashCode(objectLookups) : 0;
         result = 31 * result + (clusterSnapshot != null ? clusterSnapshot.hashCode() : 0);
         return result;
      }
   }
}
