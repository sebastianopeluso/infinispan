package org.infinispan.dataplacement;

import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Collects all the Object Lookup from all the members. In the coordinator side, it collects all the acks before
 * triggering the state transfer
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectLookupManager {

   private final List<Address> membersList;

   //the state transfer manager
   private final DistributedStateTransferManagerImpl stateTransfer;

   private int objectLookupReceived;

   private int acksReceived;

   public ObjectLookupManager(DistributedStateTransferManagerImpl stateTransfer) {
      this.stateTransfer = stateTransfer;
      membersList = new ArrayList<Address>();
      objectLookupReceived = 0;
   }

   /**
    * reset the state (before each round)
    */
   public final synchronized void resetState() {
      objectLookupReceived = 0;
      acksReceived = 0;
      stateTransfer.createDataPlacementConsistentHashing(membersList);
   }

   /**
    * updates the members list
    *
    * @param members the new members list
    */
   public final synchronized void updateMembersList(List<Address> members) {
      membersList.clear();
      membersList.addAll(members);
   }

   /**
    * add a new Object Lookup from a member
    *
    * Note: it only returns true on the first time that it is ready to the stat transfer. the following
    *       invocations return false
    *
    *
    * @param from          the creator member
    * @param objectLookup  the Object Lookup instance
    * @return              true if it has all the object lookup, false otherwise (see Note)
    */
   public final synchronized boolean addObjectLookup(Address from, Collection<ObjectLookup> objectLookup) {
      if (hasAllObjectLookup()) {
         return false;
      }
      stateTransfer.addObjectLookup(from, objectLookup);
      objectLookupReceived++;
      return hasAllObjectLookup();
   }

   /**
    * add an ack from a member
    *
    * Note: it only returns true once, when it has all the acks for the first time
    *
    * @return  true if it is has all the acks, false otherwise (see Note)
    */
   public final synchronized boolean addAck() {
      if (hasAllAcks()) {
         return false;
      }
      acksReceived++;
      return hasAllAcks();
   }

   /**
    * returns true if it has all the Object Lookup from all members
    *
    * @return  true if it has all the Object Lookup from all members
    */
   private boolean hasAllObjectLookup() {
      return membersList.size() <= objectLookupReceived;
   }

   /**
    * returns true if it has all the acks from all members
    *
    * @return  true if it has all the acks from all members
    */
   private boolean hasAllAcks() {
      return membersList.size() <= acksReceived;
   }
}
