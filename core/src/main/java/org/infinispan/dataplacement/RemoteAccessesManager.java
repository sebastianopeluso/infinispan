package org.infinispan.dataplacement;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DataPlacementConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Manages all the remote access and creates the request list to send to each other member
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
public class RemoteAccessesManager {
   private static final Log log = LogFactory.getLog(RemoteAccessesManager.class);

   private final DistributionManager distributionManager;

   private ClusterSnapshot clusterSnapshot;

   //careful with this arrays!!
   private Map[] remoteAccesses;
   private Map[] localAccesses;

   private final StreamLibContainer streamLibContainer;

   private boolean hasAccessesCalculated;

   public RemoteAccessesManager(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;

      streamLibContainer = StreamLibContainer.getInstance();
   }

   /**
    * reset the state (before each round)
    *
    * @param clusterSnapshot  the current cluster snapshot
    */
   public final synchronized void resetState(ClusterSnapshot clusterSnapshot) {
      this.clusterSnapshot = clusterSnapshot;
      remoteAccesses = new Map[clusterSnapshot.size()];
      localAccesses = new Map[clusterSnapshot.size()];
      hasAccessesCalculated = false;
   }

   /**
    * calculates the object request list to request to each member
    */
   private void calculateAccessesIfNeeded(){
      if (hasAccessesCalculated) {
         return;
      }
      hasAccessesCalculated = true;
      int minSize = (int) (streamLibContainer.getCapacity() * 0.8);

      if (log.isTraceEnabled()) {
         log.trace("Calculating accessed keys for data placement optimization");
      }

      Map<Object, Long> tempAccesses = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET);

      if (log.isDebugEnabled()) {
         log.debugf("Size of remote accesses is %s and minimum size to send the request is %s ", tempAccesses.size(),
                    minSize);
      }

      // Only send statistics if there are enough objects
      if (tempAccesses.size() >= minSize) {
         sortObjectsByPrimaryOwner(tempAccesses, remoteAccesses);
      }

      tempAccesses = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET);

      if (log.isDebugEnabled()) {
         log.debugf("Size of local accesses is %s and minimum size to send the request is %s ", tempAccesses.size(),
                    minSize);
      }

      sortObjectsByPrimaryOwner(tempAccesses, localAccesses);
   }

   /**
    * returns the request object list for the {@code member}
    *
    * @param member  the destination member
    * @return        the request object list. It can be empty if no requests are necessary
    */
   @SuppressWarnings("unchecked")
   public synchronized final ObjectRequest getObjectRequestForAddress(Address member) {
      calculateAccessesIfNeeded();
      int addressIndex = clusterSnapshot.indexOf(member);

      if (addressIndex == -1) {
         log.warnf("Trying to get Object Requests to send to %s but it does not exists in cluster snapshot %s",
                   member, clusterSnapshot);
         return new ObjectRequest(null, null);
      }

      ObjectRequest request = new ObjectRequest(remoteAccesses[addressIndex], localAccesses[addressIndex]);

      if (log.isInfoEnabled()) {
         log.debugf("Getting request list for %s. Request is %s", member, request.toString(log.isDebugEnabled()));
      }

      return request;
   }

   /**
    * sort the keys and number of access by primary owner
    *
    *
    * @param accesses      the remote accesses
    * @param membersAccess the array that in each position contains the entries to send to that member      
    */
   @SuppressWarnings("unchecked")
   private void sortObjectsByPrimaryOwner(Map<Object, Long> accesses, Map[] membersAccess) {
      Map<Object, List<Address>> primaryOwners = getDefaultConsistentHash().locateAll(accesses.keySet(), 1);

      if (log.isDebugEnabled()) {
         log.debugf("Accesses ara %s and primary owners are %s", accesses, primaryOwners);
      }

      for (Entry<Object, Long> entry : accesses.entrySet()) {
         Object key = entry.getKey();
         Address primaryOwner = primaryOwners.remove(key).get(0);
         int addressIndex = clusterSnapshot.indexOf(primaryOwner);

         if (addressIndex == -1) {
            log.warnf("Primary owner [%s] does not exists in cluster snapshot %s", primaryOwner, clusterSnapshot);
            continue;
         }

         if (membersAccess[addressIndex] == null) {
            membersAccess[addressIndex] = new HashMap<Object, Long>();
         }
         membersAccess[addressIndex].put(entry.getKey(), entry.getValue());
      }
   }

   /**
    * returns the actual consistent hashing
    *
    * @return  the actual consistent hashing
    */
   private ConsistentHash getDefaultConsistentHash() {
      ConsistentHash hash = this.distributionManager.getConsistentHash();
      return hash instanceof DataPlacementConsistentHash ?
            ((DataPlacementConsistentHash) hash).getDefaultHash() :
            hash;
   }
}
