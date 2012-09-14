package org.infinispan.dataplacement;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * // TODO: Document this
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectPlacementManagerV2 {

   private static final Log log = LogFactory.getLog(ObjectPlacementManager.class);

   //contains the list of members (same order in all nodes)
   private final List<Address> addressList;

   //<node index, <key, number of accesses>
   private final Map<Integer, Map<Object, Long>> remoteRequests;

   //<node index, <key, number of accesses>
   private final Map<Integer, Map<Object, Long>> localRequests;

   private boolean hasReceivedAllRequests;

   //this can be quite big. save it as an array to save some memory
   private Object[] allKeysMoved;

   private final DistributionManager distributionManager;
   private final Hash hash;

   public ObjectPlacementManagerV2(DistributionManager distributionManager, Hash hash){
      this.distributionManager = distributionManager;
      this.hash = hash;

      addressList = new ArrayList<Address>();
      remoteRequests = new TreeMap<Integer, Map<Object, Long>>();
      localRequests = new TreeMap<Integer, Map<Object, Long>>();
      hasReceivedAllRequests = false;
   }

   /**
    * reset the state (before each round)
    */
   public final synchronized void resetState() {
      remoteRequests.clear();
      localRequests.clear();
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

   public final synchronized boolean aggregateRequest(Address member, ObjectRequest objectRequest) {
      if (hasReceivedAllRequests) {
         return false;
      }

      int senderID = addressList.indexOf(member);

      if (senderID < 0) {
         log.warnf("Received request list from %s but it does not exits", member);
         return false;
      }

      remoteRequests.put(senderID, objectRequest.getRemoteAccesses());
      localRequests.put(senderID, objectRequest.getLocalAccesses());
      hasReceivedAllRequests = addressList.size() <= remoteRequests.size();

      if (log.isDebugEnabled()) {
         log.debugf("Received request list from %s. Received from %s nodes and expects %s. Remote keys are %s and " +
                          "local keys are %s", member, remoteRequests.size(), addressList.size(),
                    objectRequest.getRemoteAccesses(), objectRequest.getLocalAccesses());
      } else {
         log.infof("Received request list from %s. Received from %s nodes and expects %s", member,
                   remoteRequests.size(), addressList.size());
      }

      return hasReceivedAllRequests;
   }

   public final synchronized Map<Object, OwnersInfo> calculateObjectsToMove() {
      Map<Object, OwnersInfo> newOwnersMap = new HashMap<Object, OwnersInfo>();

      for (int requesterIdx = 0; requesterIdx < addressList.size(); ++requesterIdx) {
         Map<Object, Long> requestedObjects = remoteRequests.remove(requesterIdx);

         if (requestedObjects == null) {
            continue;
         }

         for (Map.Entry<Object, Long> entry : requestedObjects.entrySet()) {
            calculateNewOwners(newOwnersMap, entry.getKey(), entry.getValue(), requesterIdx);
         }
         //release memory asap
         requestedObjects.clear();
      }
      //release memory asap
      remoteRequests.clear();
      localRequests.clear();

      //process the old moved keys. this will set the new owners of the previous rounds
      for (Object key : allKeysMoved) {
         if (!newOwnersMap.containsKey(key)) {
            newOwnersMap.put(key, createOwnersInfo(key));
         }
      }

      //update all the keys moved array
      allKeysMoved = newOwnersMap.keySet().toArray(new Object[newOwnersMap.size()]);

      return newOwnersMap;
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
            ownerIndex = findNewOwner(key, replicas);
         }

         Long accesses = localAccesses.remove(ownerIndex);

         if (accesses == null) {
            accesses = 0L;
         }

         ownersInfo.add(ownerIndex, accesses);
      }

      return ownersInfo;
   }

   private int findNewOwner(Object key, Collection<Address> alreadyOwner) {
      int size = addressList.size();

      if (size <= 1) {
         return 0;
      }

      int startIndex = hash.hash(key) % size;

      for (int index = startIndex + 1; index != startIndex; index = (index + 1) % size) {
         if (!alreadyOwner.contains(addressList.get(index))) {
            return index;
         }
         index += (index + 1) % size;
      }

      return 0;
   }

}

