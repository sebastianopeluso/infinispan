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

   private final Map<Address, Map<Object, Long>> remoteAccessPerAddress;
   private final Map<Address, Map<Object, Long>> localAccessPerAddress;
   private final StreamLibContainer streamLibContainer;

   private boolean hasAccessesCalculated;

   public RemoteAccessesManager(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;

      streamLibContainer = StreamLibContainer.getInstance();
      remoteAccessPerAddress = new HashMap<Address, Map<Object, Long>>();
      localAccessPerAddress = new HashMap<Address, Map<Object, Long>>();
   }

   /**
    * reset the state (before each round)
    */
   public final synchronized void resetState() {
      remoteAccessPerAddress.clear();
      localAccessPerAddress.clear();
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
         remoteAccessPerAddress.putAll(sortObjectsByPrimaryOwner(tempAccesses));
      }

      tempAccesses = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET);

      if (log.isDebugEnabled()) {
         log.debugf("Size of local accesses is %s and minimum size to send the request is %s ", tempAccesses.size(),
                    minSize);
      }

      if (tempAccesses.size() >= streamLibContainer.getCapacity() * 0.8) {
         localAccessPerAddress.putAll(sortObjectsByPrimaryOwner(tempAccesses));
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
      ObjectRequest request = new ObjectRequest(remoteAccessPerAddress.remove(member), localAccessPerAddress.remove(member));

      if (log.isInfoEnabled()) {
         log.debugf("Getting request list for %s. Request is %s", member, request.toString(log.isDebugEnabled()));
      }

      return request;
   }

   /**
    * sort the keys and number of access by primary owner
    *
    * @param accesses   the remote accesses
    * @return           the map between primary owner and object and number of accesses 
    */
   private Map<Address, Map<Object, Long>> sortObjectsByPrimaryOwner(Map<Object, Long> accesses) {
      Map<Address, Map<Object, Long>> objectLists = new HashMap<Address, Map<Object, Long>>();
      Map<Object, List<Address>> primaryOwners = getDefaultConsistentHash().locateAll(accesses.keySet(), 1);

      if (log.isDebugEnabled()) {
         log.debugf("Accesses ara %s and primary owners are %s", accesses, primaryOwners);
      }

      for (Entry<Object, Long> entry : accesses.entrySet()) {
         Object key = entry.getKey();
         Address primaryOwner = primaryOwners.remove(key).get(0);

         if (!objectLists.containsKey(primaryOwner)) {
            objectLists.put(primaryOwner, new HashMap<Object, Long>());
         }
         objectLists.get(primaryOwner).put(entry.getKey(), entry.getValue());
      }

      log.infof("List sorted. Number of primary owners is %s", objectLists.size());

      if (log.isTraceEnabled()) {
         log.tracef("Numbers of keys sent by primary owner");
         for (Entry<Object, List<Address>> entry : primaryOwners.entrySet()) {
            log.tracef("Primary Owner: %s, number of keys requested: %s", entry.getKey(), entry.getValue().size());
         }
      }
      return objectLists;
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
