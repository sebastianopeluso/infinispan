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
    * calculates the remote request list to send for each member
    */
   private void calculateAccessesIfNeeded(){
      if (hasAccessesCalculated) {
         return;
      }
      hasAccessesCalculated = true;

      Map<Object, Long> tempAccesses = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET);

      log.info("Size of Remote Get is: " + tempAccesses.size());

      // Only send statistics if there are enough objects
      if (tempAccesses.size() >= streamLibContainer.getCapacity() * 0.8) {
         remoteAccessPerAddress.putAll(sortObjectsByPrimaryOwner(tempAccesses));
      }

      tempAccesses = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET);

      if (tempAccesses.size() >= streamLibContainer.getCapacity() * 0.8) {
         localAccessPerAddress.putAll(sortObjectsByPrimaryOwner(tempAccesses));
      }
   }

   /**
    * returns the remote request list to send for the member
    * @param member  the member to send the list
    * @return        the request list, object and number of accesses, or null if it has no accesses to that member
    */
   public synchronized final ObjectRequest getObjectRequestForAddress(Address member) {
      calculateAccessesIfNeeded();
      ObjectRequest request = new ObjectRequest(remoteAccessPerAddress.remove(member), localAccessPerAddress.remove(member));

      if (log.isInfoEnabled()) {
         log.debugf("Getting request list to send to %s. Request is %s", member, request.toString(log.isDebugEnabled()));
      }

      return request;
   }

   /**
    * sort the keys and access sorted by owner
    *
    *
    * @param remoteGet        the remote accesses
    * @return                 the map between owner and each object and number of access 
    */
   private Map<Address, Map<Object, Long>> sortObjectsByPrimaryOwner(Map<Object, Long> remoteGet) {
      Map<Address, Map<Object, Long>> objectLists = new HashMap<Address, Map<Object, Long>>();
      Map<Object, List<Address>> mappedObjects = getDefaultConsistentHash().locateAll(remoteGet.keySet(), 1);

      Address address;
      Object key;

      for (Entry<Object, Long> entry : remoteGet.entrySet()) {
         key = entry.getKey();
         address = mappedObjects.remove(key).get(0);

         if (!objectLists.containsKey(address)) {
            objectLists.put(address, new HashMap<Object, Long>());
         }
         objectLists.get(address).put(entry.getKey(), entry.getValue());
      }

      log.infof("List sorted. Number of primary owners is %s", objectLists.size());
      if (log.isDebugEnabled()) {
         for(Entry<Address,Map<Object, Long>> map : objectLists.entrySet()){
            log.debugf("%s: %s keys requested", map.getKey(), map.getValue().size());
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
