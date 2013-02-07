package org.infinispan.dataplacement;

import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.BitSet;
import java.util.Map;

/**
 * Collects all the Object Lookup from all the members. In the coordinator side, it collects all the acks before
 * triggering the state transfer
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectLookupManager {

   private static final Log log = LogFactory.getLog(ObjectLookupManager.class);

   private ClusterSnapshot clusterSnapshot;

   private final BitSet objectLookupReceived;

   private final BitSet acksReceived;
   
   private ObjectLookup[] receivedObjectLookup;

   public ObjectLookupManager() {
      objectLookupReceived = new BitSet();
      acksReceived = new BitSet();
   }

   /**
    * reset the state (before each round)
    *
    * @param roundClusterSnapshot the current cluster members                    
    */
   public final synchronized void resetState(ClusterSnapshot roundClusterSnapshot, int numberOfSegments) {
      clusterSnapshot = roundClusterSnapshot;
      objectLookupReceived.clear();
      acksReceived.clear();
      receivedObjectLookup = new ObjectLookup[numberOfSegments];
   }

   /**
    * add a new Object Lookup from a member
    *
    * Note: it only returns true on the first time that it is ready to the stat transfer. the following
    *       invocations return false
    *
    * @param from          the creator member
    * @param segmentObjectLookups  the Object Lookup instance
    * @return              true if it has all the object lookup, false otherwise (see Note)
    */
   public final synchronized boolean addObjectLookup(Address from, Map<Integer, ObjectLookup> segmentObjectLookups) {
      if (hasAllObjectLookup()) {
         return false;
      }

      int senderId = clusterSnapshot.indexOf(from);

      if (senderId < 0) {
         log.warnf("Receive an object lookup from %s but it is not in members list %s", from, clusterSnapshot);
         return false;
      }

      for (Map.Entry<Integer, ObjectLookup> entry : segmentObjectLookups.entrySet()) {
         if (entry.getKey() < 0 || entry.getKey() >= receivedObjectLookup.length) {
            throw new IllegalStateException("This should not happen");
         }
         receivedObjectLookup[entry.getKey()] = entry.getValue();
      }      
      objectLookupReceived.set(senderId);

      logObjectLookupReceived(from);

      return hasAllObjectLookup();
   }

   /**
    * add an ack from a member
    *
    * Note: it only returns true once, when it has all the acks for the first time
    *
    * @param from the sender
    * @return     true if it is has all the acks, false otherwise (see Note)    
    */
   public final synchronized boolean addAck(Address from) {
      if (hasAllAcks()) {
         return false;
      }

      int senderId = clusterSnapshot.indexOf(from);

      if (senderId < 0) {
         log.warnf("Receive an ack from %s but it is not in members list %s", from, clusterSnapshot);
         return false;
      }

      acksReceived.set(senderId);

      logAckReceived(from);

      return hasAllAcks();
   }

   /**
    * returns true if it has all the Object Lookup from all members
    *
    * @return  true if it has all the Object Lookup from all members
    */
   private boolean hasAllObjectLookup() {
      return clusterSnapshot.size() == objectLookupReceived.cardinality();
   }

   /**
    * returns true if it has all the acks from all members
    *
    * @return  true if it has all the acks from all members
    */
   private boolean hasAllAcks() {
      return clusterSnapshot.size() == acksReceived.cardinality();
   }

   private void logObjectLookupReceived(Address from) {
      if (log.isDebugEnabled()) {
         log.debugf("Objects lookup received from %s. Missing objects lookup are %s",
                    from, (clusterSnapshot.size() - objectLookupReceived.cardinality()));
      }
   }

   private void logAckReceived(Address from) {
      if (log.isDebugEnabled()) {
         StringBuilder missingMembers = new StringBuilder();

         for (int i = 0; i < clusterSnapshot.size(); ++i) {
            if (!acksReceived.get(i)) {
               missingMembers.append(clusterSnapshot.get(i)).append(" ");
            }
         }
         log.debugf("Ack received from %s. Missing ack are %s", from, missingMembers);
      }
   }
}
