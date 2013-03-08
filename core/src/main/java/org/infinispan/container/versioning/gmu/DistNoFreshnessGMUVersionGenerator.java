package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersion;

/**
 *
 *
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class DistNoFreshnessGMUVersionGenerator extends DistGMUVersionGenerator{


   public DistNoFreshnessGMUVersionGenerator() {
      super();
   }


   @Override
   public GMUVersion calculateMaxVersionToRead(EntryVersion transactionVersion,
                                               Collection<Address> alreadyReadFrom) {

      //I want to ignore here the alreadyReadFromSet. I want to fix all the entries in the transaction version to avoid reading in the commit log.
      //In this case, I don't maximize data freshness during read operation.


      return updatedVersion(transactionVersion);

   }

   @Override
   public GMUReadVersion convertVersionToRead(EntryVersion version) {
      if (version == null) {
         return null;
      }
      GMUVersion gmuVersion = toGMUVersion(version);

      long value = gmuVersion.getThisNodeVersionValue();

      if(value == GMUVersion.NON_EXISTING){
         log.warn("GMU without freshness: trying to read on a node for which I have never seen a commit. Set associated version value to 0");

         value = 0;
      }
      return new GMUReadVersion(cacheName, currentViewId, this, value);
   }
}
