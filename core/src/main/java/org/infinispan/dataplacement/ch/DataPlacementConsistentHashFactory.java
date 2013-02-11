package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConsistentHashFactory<CH extends ConsistentHash>
      implements ConsistentHashFactory<DataPlacementConsistentHash<CH>> {

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

   @Override
   public DataPlacementConsistentHash<CH> rebalanceAutoPlacer(DataPlacementConsistentHash<CH> baseCH,
                                                              ObjectLookup[] segmentMappings,
                                                              ClusterSnapshot clusterSnapshot) {
      CH ch = consistentHashFactory.rebalance(baseCH.getConsistentHash());
      if (ch.equals(baseCH.getConsistentHash())) {
         return new DataPlacementConsistentHash<CH>(baseCH.getConsistentHash(), segmentMappings, clusterSnapshot);
      }
      return new DataPlacementConsistentHash<CH>(ch, segmentMappings, clusterSnapshot);
   }

   public static class Externalizer extends AbstractExternalizer<DataPlacementConsistentHashFactory> {

      @Override
      public Set<Class<? extends DataPlacementConsistentHashFactory>> getTypeClasses() {
         return Util.<Class<? extends DataPlacementConsistentHashFactory>>asSet(DataPlacementConsistentHashFactory.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DataPlacementConsistentHashFactory object) throws IOException {
         output.writeObject(object.consistentHashFactory);
      }

      @Override
      public DataPlacementConsistentHashFactory readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ConsistentHashFactory factory = (ConsistentHashFactory) input.readObject();
         return new DataPlacementConsistentHashFactory(factory);
      }

      @Override
      public Integer getId() {
         return Ids.DATA_PLACEMENT_CONSISTENT_HASH_FACTORY;
      }
   }
}
