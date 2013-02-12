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

import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementRebalancePolicy implements RebalancePolicy {

   private static final Log log = LogFactory.getLog(DataPlacementRebalancePolicy.class);
   private final ConcurrentMap<String, Object> newSegmentMappings = ConcurrentMapFactory.makeConcurrentMap();
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

      if (!cacheStatus.hasJoiners() && isBalanced(cacheStatus.getCacheTopology().getCurrentCH()) &&
            isSegmentsMappingsApplied(cacheStatus.getCacheTopology().getCurrentCH(), newSegmentMappings.get(cacheName))) {
         log.tracef("Not triggering rebalance for cache %s, no joiners and the current consistent hash is already balanced",
                    cacheName);
         newSegmentMappings.remove(cacheName); //TODO synchronize this!
         return;
      }

      if (cacheStatus.isRebalanceInProgress()) {
         log.tracef("Not triggering rebalance for cache %s, a rebalance is already in progress", cacheName);
         return;
      }

      log.tracef("Triggering rebalance for cache %s", cacheName);
      clusterTopologyManager.triggerRebalance(cacheName, newSegmentMappings.get(cacheName)); //TODO synchronize this!
   }

   private boolean isSegmentsMappingsApplied(ConsistentHash currentCH, Object mappings) {
      if (currentCH instanceof DataPlacementConsistentHash) {
         //TODO get current mapping
         //TODO check with mappings
      }
      return true;
   }

   public void setNewSegmentMappings(String cacheName, Object segmentMappings) throws Exception {
      newSegmentMappings.put(cacheName, segmentMappings);      
      clusterTopologyManager.triggerRebalance(cacheName, segmentMappings);
   }

   private boolean isBalanced(ConsistentHash ch) {
      int numSegments = ch.getNumSegments();
      for (int i = 0; i < numSegments; i++) {
         int actualNumOwners = Math.min(ch.getMembers().size(), ch.getNumOwners());
         if (ch.locateOwnersForSegment(i).size() != actualNumOwners) {
            return false;
         }
      }
      return true;
   }
}
