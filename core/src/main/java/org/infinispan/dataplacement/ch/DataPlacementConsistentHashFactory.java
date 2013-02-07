package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConsistentHashFactory<CH extends ConsistentHash> 
      implements ConsistentHashFactory<DataPlacementConsistentHash<CH>>{
   
   private final ConsistentHashFactory<CH> consistentHashFactory;

   public DataPlacementConsistentHashFactory(ConsistentHashFactory<CH> consistentHashFactory) {
      this.consistentHashFactory = consistentHashFactory;
   }

   @Override
   public DataPlacementConsistentHash<CH> create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      CH ch = consistentHashFactory.create(hashFunction, numOwners, numSegments, members);
      return new DataPlacementConsistentHash<CH>(ch, null, null);
   }

   @Override
   public DataPlacementConsistentHash<CH> updateMembers(DataPlacementConsistentHash<CH> baseCH, List<Address> newMembers) {      
      CH ch = consistentHashFactory.updateMembers(baseCH.getConsistentHash(), newMembers);
      return new DataPlacementConsistentHash<CH>(baseCH, ch);
   }

   @Override
   public DataPlacementConsistentHash<CH> rebalance(DataPlacementConsistentHash<CH> baseCH) {
      CH ch = consistentHashFactory.rebalance(baseCH.getConsistentHash());
      if (ch.equals(baseCH.getConsistentHash())) {
         return baseCH;
      }
      return new DataPlacementConsistentHash<CH>(baseCH, ch);
   }

   @Override
   public DataPlacementConsistentHash<CH> union(DataPlacementConsistentHash<CH> ch1, DataPlacementConsistentHash<CH> ch2) {
      return null;  // TODO: Customise this generated block
   }
}
