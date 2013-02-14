package org.infinispan.dataplacement;

import org.infinispan.dataplacement.ch.DataPlacementConsistentHash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.RebalancePolicy;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementRebalancePolicy implements RebalancePolicy {

   private static final Log log = LogFactory.getLog(DataPlacementRebalancePolicy.class);
   private final ConcurrentMap<String, ClusterObjectLookup> newSegmentMappings = ConcurrentMapFactory.makeConcurrentMap();
   private ClusterTopologyManager clusterTopologyManager;

   @Inject
   public void setClusterTopologyManager(ClusterTopologyManager clusterTopologyManager) {
      this.clusterTopologyManager = clusterTopologyManager;
   }

   @Override
   public void initCache(String cacheName, ClusterCacheStatus cacheStatus) throws Exception {
      //nothing to do
   }

   @Override
   public void updateCacheStatus(String cacheName, ClusterCacheStatus cacheStatus) throws Exception {
      log.tracef("Cache %s status changed: joiners=%s, topology=%s", cacheName, cacheStatus.getJoiners(),
                 cacheStatus.getCacheTopology());
      if (!cacheStatus.hasMembers()) {
         log.tracef("Not triggering rebalance for zero-members cache %s", cacheName);
         return;
      }
      
      if (!cacheStatus.hasJoiners() && isBalanced(cacheName, cacheStatus.getCacheTopology().getCurrentCH())) {
         log.tracef("Not triggering rebalance for cache %s, no joiners and the current consistent hash is already balanced",
                    cacheName);         
         return;
      }

      if (cacheStatus.isRebalanceInProgress()) {
         log.tracef("Not triggering rebalance for cache %s, a rebalance is already in progress", cacheName);
         return;
      }

      ClusterObjectLookup clusterObjectLookup = getSegmentsToApply(cacheName, cacheStatus.getCacheTopology().getCurrentCH());
      log.tracef("Triggering rebalance for cache %s", cacheName);
      clusterTopologyManager.triggerRebalance(cacheName, clusterObjectLookup);
   }

   private ClusterObjectLookup getSegmentsToApply(String cacheName, ConsistentHash consistentHash) {
      if (consistentHash instanceof DataPlacementConsistentHash) {
         ClusterObjectLookup clusterObjectLookup = newSegmentMappings.get(cacheName);
         List<ClusterObjectLookup> list = ((DataPlacementConsistentHash) consistentHash).getClusterObjectLookupList();
         if (clusterObjectLookup != null && list.size() == 1 && clusterObjectLookup.equals(list.get(0))) {
            //in this case, the ConsistentHash already has the most recent mappings. remove from the newMapping map
            newSegmentMappings.remove(cacheName, clusterObjectLookup);
            return null;
         }
         return clusterObjectLookup;
      }
      return null;
   }

   public void setNewSegmentMappings(String cacheName, ClusterObjectLookup segmentMappings) throws Exception {
      newSegmentMappings.put(cacheName, segmentMappings);
      clusterTopologyManager.triggerRebalance(cacheName, segmentMappings);
   }

   public boolean isBalanced(String cacheName, ConsistentHash ch) {
      int numSegments = ch.getNumSegments();
      for (int i = 0; i < numSegments; i++) {
         int actualNumOwners = Math.min(ch.getMembers().size(), ch.getNumOwners());
         if (ch.locateOwnersForSegment(i).size() != actualNumOwners) {
            return false;
         }
      }
      ClusterObjectLookup clusterObjectLookup = getSegmentsToApply(cacheName, ch);
      return clusterObjectLookup == null;
   }
}
