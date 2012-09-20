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
public class AccessesManager {
   private static final Log log = LogFactory.getLog(AccessesManager.class);

   private final DistributionManager distributionManager;

   private ClusterSnapshot clusterSnapshot;

   private Accesses[] accessesByPrimaryOwner;

   private final StreamLibContainer streamLibContainer;

   private boolean hasAccessesCalculated;

   public AccessesManager(DistributionManager distributionManager) {
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
      accessesByPrimaryOwner = new Accesses[clusterSnapshot.size()];
      for (int i = 0; i < accessesByPrimaryOwner.length; ++i) {
         accessesByPrimaryOwner[i] = new Accesses();
      }
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

      if (log.isTraceEnabled()) {
         log.trace("Calculating accessed keys for data placement optimization");
      }

      Map<Object, Long> tempAccesses = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET);

      sortObjectsByPrimaryOwner(tempAccesses, true);

      tempAccesses = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET);

      sortObjectsByPrimaryOwner(tempAccesses, false);

      if (log.isTraceEnabled()) {
         StringBuilder stringBuilder = new StringBuilder("Accesses:\n");
         for (int i = 0; i < accessesByPrimaryOwner.length; ++i) {
            stringBuilder.append(clusterSnapshot.get(i)).append(" ==> ").append(accessesByPrimaryOwner[i]).append("\n");
         }
         log.debug(stringBuilder);
      }
   }

   /**
    * returns the request object list for the {@code member}
    *
    * @param member  the destination member
    * @return        the request object list. It can be empty if no requests are necessary
    */
   public synchronized final ObjectRequest getObjectRequestForAddress(Address member) {
      calculateAccessesIfNeeded();
      int addressIndex = clusterSnapshot.indexOf(member);

      if (addressIndex == -1) {
         log.warnf("Trying to get Object Requests to send to %s but it does not exists in cluster snapshot %s",
                   member, clusterSnapshot);
         return new ObjectRequest(null, null);
      }

      ObjectRequest request = accessesByPrimaryOwner[addressIndex].toObjectRequest();

      if (log.isInfoEnabled()) {
         log.debugf("Getting request list for %s. Request is %s", member, request.toString(log.isDebugEnabled()));
      }

      return request;
   }

   /**
    * sort the keys and number of access by primary owner
    *
    *
    * @param accesses   the remote accesses
    * @param remote     true if the accesses to process are from remote access, false otherwise      
    */
   @SuppressWarnings("unchecked")
   private void sortObjectsByPrimaryOwner(Map<Object, Long> accesses, boolean remote) {
      Map<Object, List<Address>> primaryOwners = getDefaultConsistentHash().locateAll(accesses.keySet(), 1);

      if (log.isDebugEnabled()) {
         log.debugf("Accesses are %s and primary owners are %s", accesses, primaryOwners);
      }

      for (Entry<Object, Long> entry : accesses.entrySet()) {
         Object key = entry.getKey();
         Address primaryOwner = primaryOwners.remove(key).get(0);
         int addressIndex = clusterSnapshot.indexOf(primaryOwner);

         if (addressIndex == -1) {
            log.warnf("Primary owner [%s] does not exists in cluster snapshot %s", primaryOwner, clusterSnapshot);
            continue;
         }

         accessesByPrimaryOwner[addressIndex].add(entry.getKey(), entry.getValue(), remote);
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

   private class Accesses {
      private final Map<Object, Long> localAccesses;
      private final Map<Object, Long> remoteAccesses;

      private Accesses() {
         localAccesses = new HashMap<Object, Long>();
         remoteAccesses = new HashMap<Object, Long>();
      }

      private void add(Object key, long accesses, boolean remote) {
         Map<Object, Long> toPut = remote ? remoteAccesses : localAccesses;
         toPut.put(key, accesses);
      }

      private ObjectRequest toObjectRequest() {
         return new ObjectRequest(remoteAccesses.size() == 0 ? null : remoteAccesses,
                                  localAccesses.size() == 0 ? null : localAccesses);
      }

      @Override
      public String toString() {
         return "Accesses{" +
               "localAccesses=" + localAccesses.size() +
               ", remoteAccesses=" + remoteAccesses.size() +
               '}';
      }
   }
}
