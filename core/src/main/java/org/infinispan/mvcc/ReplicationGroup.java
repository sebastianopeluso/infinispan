package org.infinispan.mvcc;

import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class ReplicationGroup {
   private int id;
   private List<Address> members;

   public ReplicationGroup(int id, List<Address> addrs) {
      this.id = id;
      this.members = addrs;
   }

   public int getId() {
      return id;
   }

   public List<Address> getMembers() {
      return members;
   }

   @Override
   public String toString() {
      return "ReplicationGroup{id=" + id + ",members=" + members + "}";
   }
}
