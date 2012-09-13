package org.infinispan.dataplacement;

import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectPlacementManagerV2 {

   private static final Log log = LogFactory.getLog(ObjectPlacementManager.class);   

   //contains the list of members (same order in all nodes)
   private final List<Address> addressList;

   //The object that were sent out. The left of pair is the destination, the right is the number that it is moved
   private final Map<Object, Pair<Integer, Integer>> allSentObjects;

   //<node index, <key, number of accesses>
   private final Map<Integer, Map<Object, Long>> remoteRequests;

   //<node index, <key, number of accesses>
   private final Map<Integer, Map<Object, Long>> localRequests;

   //<key, destination node index>
   private final Map<Object, Integer> objectsToMove;

   private boolean hasReceivedAllRequests;

   private final DataContainer dataContainer;
   private final DistributionManager distributionManager;

   public ObjectPlacementManagerV2(DistributionManager distributionManager, DataContainer dataContainer){      
      this.dataContainer = dataContainer;
      this.distributionManager = distributionManager;

      addressList = new ArrayList<Address>();
      allSentObjects = new HashMap<Object, Pair<Integer, Integer>>();
      remoteRequests = new TreeMap<Integer, Map<Object, Long>>();
      localRequests = new TreeMap<Integer, Map<Object, Long>>();
      objectsToMove = new HashMap<Object, Integer>();
      hasReceivedAllRequests = false;
   }

   /**
    * reset the state (before each round)
    */
   public final synchronized void resetState() {
      remoteRequests.clear();
      hasReceivedAllRequests = false;
   }

   /**
    * updates the members list
    *
    * @param addresses  the new member list
    */
   public final synchronized void updateMembersList(List<Address> addresses) {
      addressList.clear();
      addressList.addAll(addresses);
   }

   public final synchronized boolean aggregateRequest(Address member, Map<Object, Long> remote, Map<Object, Long> local) {
      if (hasReceivedAllRequests) {
         return false;
      }

      int senderID = addressList.indexOf(member);

      if (senderID < 0) {
         log.warnf("Received request list from %s but it does not exits", member);
         return false;
      }

      remoteRequests.put(senderID, remote == null ? Collections.<Object, Long>emptyMap() : remote);
      localRequests.put(senderID, local == null ? Collections.<Object, Long>emptyMap() : local);
      hasReceivedAllRequests = addressList.size() <= remoteRequests.size();

      if (log.isDebugEnabled()) {
         log.debugf("Received request list from %s. Received from %s nodes and expects %s. Remote keys are %s and " +
                          "local keys are %s", member, remoteRequests.size(), addressList.size(), remote, local);
      } else {
         log.infof("Received request list from %s. Received from %s nodes and expects %s", member,
                   remoteRequests.size(), addressList.size());
      }

      return hasReceivedAllRequests;
   }

   private void calculateObjectsToMove() {
      Map<Object, OwnersInfo> newOwnersMap = new HashMap<Object, OwnersInfo>();
      for (Map.Entry<Integer, Map<Object, Long>> remoteEntry : remoteRequests.entrySet()) {
         int requesterIdx = remoteEntry.getKey();
         for (Map.Entry<Object, Long> entry : remoteEntry.getValue().entrySet()) {
            calculateNewOwners(newOwnersMap, entry.getKey(), entry.getValue(), requesterIdx);
         }          
      }
   }

   private void calculateNewOwners(Map<Object, OwnersInfo> newOwnersMap, Object key, long numberOfRequests, int requesterId) {
      OwnersInfo newOwnersInfo = newOwnersMap.get(key);

      if (newOwnersInfo == null) {                  
         newOwnersInfo = createOwnersInfo(key);                        
         newOwnersMap.put(key, newOwnersInfo);
      }
      newOwnersInfo.calculateNewOwner(requesterId, numberOfRequests);  
   }

   private Map<Integer, Long> getLocalAccesses(Object key) {
      Map<Integer, Long> localAccessesMap = new TreeMap<Integer, Long>();
      
      for (Map.Entry<Integer, Map<Object, Long>> entry : localRequests.entrySet()) {
         int localNodeIndex = entry.getKey();
         Long localAccesses = entry.getValue().remove(key);
         
         if (localAccesses != null) {
            localAccessesMap.put(localNodeIndex, localAccesses);
         }                  
      }
      return localAccessesMap;
   }
   
   private OwnersInfo createOwnersInfo(Object key) {
      Collection<Address> replicas = distributionManager.locate(key);      
      Map<Integer, Long> localAccesses = getLocalAccesses(key);
      
      OwnersInfo ownersInfo = new OwnersInfo(replicas.size());
      
      for (Address currentOwner : replicas) {
         int ownerIndex = addressList.indexOf(currentOwner);
         
         if (ownerIndex == -1) {
            continue; //TODO WARNING!
         }
         
         Long accesses = localAccesses.remove(ownerIndex);
         
         if (accesses == null) {
            accesses = 0L;
         }
         
         ownersInfo.add(ownerIndex, accesses);
      }
      
      return ownersInfo;
   }

}

