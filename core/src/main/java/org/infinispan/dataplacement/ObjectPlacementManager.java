package org.infinispan.dataplacement;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DataPlacementConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Collects all the remote and local access for each member for the key in which this member is the 
 * primary owner
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectPlacementManager {

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
   private final int defaultNumberOfOwners;

   public ObjectPlacementManager(DistributionManager distributionManager, Hash hash, int defaultNumberOfOwners){
      this.distributionManager = distributionManager;
      this.hash = hash;
      this.defaultNumberOfOwners = defaultNumberOfOwners;

      addressList = new ArrayList<Address>();
      remoteRequests = new TreeMap<Integer, Map<Object, Long>>();
      localRequests = new TreeMap<Integer, Map<Object, Long>>();
      hasReceivedAllRequests = false;
      allKeysMoved = new Object[0];
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

   /**
    * collects the local and remote accesses for each member
    *
    * @param member        the member that sent the {@code objectRequest}
    * @param objectRequest the local and remote accesses
    * @return              true if all requests are received, false otherwise. It only returns true on the first
    *                      time it has all the objects
    */
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
         log.debugf("Received request list from %s. Received from %s nodes and expects %s. Request is %s", member,
                    remoteRequests.size(), addressList.size(), objectRequest.toString(log.isTraceEnabled()));
      }

      return hasReceivedAllRequests;
   }

   /**
    * calculate the new owners based on the requests received.
    *
    * @return  a map with the keys to be moved and the new owners
    */
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

      removeNotMovedObjects(newOwnersMap);

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

   /**
    * for each object to move, it checks if the owners are different from the owners returned by the original
    * Infinispan's consistent hash. If this is true, the object is removed from the map {@code newOwnersMap}
    *
    * @param newOwnersMap  the map with the key to be moved and the new owners
    */
   private void removeNotMovedObjects(Map<Object, OwnersInfo> newOwnersMap) {
      ConsistentHash defaultConsistentHash = getDefaultConsistentHash();
      Iterator<Map.Entry<Object, OwnersInfo>> iterator = newOwnersMap.entrySet().iterator();

      //if the owners info corresponds to the default consistent hash owners, remove the key from the map 
      mainLoop: while (iterator.hasNext()) {
         Map.Entry<Object, OwnersInfo> entry = iterator.next();
         Object key = entry.getKey();
         OwnersInfo ownersInfo = entry.getValue();
         Collection<Integer> ownerInfoIndexes = ownersInfo.getNewOwnersIndexes();
         Collection<Address> defaultOwners = defaultConsistentHash.locate(key, defaultNumberOfOwners);

         if (ownerInfoIndexes.size() != defaultOwners.size()) {
            continue;
         }

         for (Address address : defaultOwners) {
            if (!ownerInfoIndexes.contains(addressList.indexOf(address))) {
               continue mainLoop;
            }
         }
         iterator.remove();
      }
   }

   /**
    * updates the owner information for the {@code key} based in the {@code numberOfRequests} made by the member who
    * requested this {@code key} (identified by {@code requesterId})
    *
    * @param newOwnersMap     the new owners map to be updated
    * @param key              the key requested
    * @param numberOfRequests the number of accesses made to this key
    * @param requesterId      the member id
    */
   private void calculateNewOwners(Map<Object, OwnersInfo> newOwnersMap, Object key, long numberOfRequests, int requesterId) {
      OwnersInfo newOwnersInfo = newOwnersMap.get(key);

      if (newOwnersInfo == null) {
         newOwnersInfo = createOwnersInfo(key);
         newOwnersMap.put(key, newOwnersInfo);
      }
      newOwnersInfo.calculateNewOwner(requesterId, numberOfRequests);
   }

   /**
    * returns the local accesses and owners for the {@code key}
    *
    * @param key  the key
    * @return     the local accesses and owners for the key     
    */
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

   /**
    * creates a new owners information initialized with the current owners returned by the current consistent hash
    * and their number of accesses for the {@code key}
    *
    * @param key  the key
    * @return     the new owners information.
    */
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
            //TODO check if this should be zero or the min number of local accesses from the member
            accesses = 0L;
         }

         ownersInfo.add(ownerIndex, accesses);
      }

      return ownersInfo;
   }

   /**
    * finds the new owner for the {@code key} based on the Infinispan's consistent hash. this is invoked
    * when the one or more current owners are not in the cluster anymore and it is necessary to find new owners
    * to respect the default number of owners per key
    *
    * @param key           the key
    * @param alreadyOwner  the current owners
    * @return              the new owner index
    */
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
         index = (index + 1) % size;
      }

      return 0;
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

