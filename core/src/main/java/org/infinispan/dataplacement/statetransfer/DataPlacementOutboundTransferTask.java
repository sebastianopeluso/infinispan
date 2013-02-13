package org.infinispan.dataplacement.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutboundTransferTask;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateProviderImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class DataPlacementOutboundTransferTask extends OutboundTransferTask {

   public DataPlacementOutboundTransferTask(Address destination, Set<Integer> segments, int stateTransferChunkSize,
                                            int topologyId, ConsistentHash readCh, StateProviderImpl stateProvider,
                                            ConsistentHash writeCh, DataContainer dataContainer,
                                            CacheLoaderManager cacheLoaderManager, RpcManager rpcManager,
                                            CommandsFactory commandsFactory, long timeout, String cacheName) {
      super(destination, segments, stateTransferChunkSize, topologyId, readCh, stateProvider, writeCh, dataContainer,
            cacheLoaderManager, rpcManager, commandsFactory, timeout, cacheName);
   }

   @Override
   protected boolean isKeyMoved(Object key) {
      int segmentId = readCh.getSegment(key);
      if (segments == null) {
         //Data Placement
         boolean isSegmentMoved = !readCh.getSegmentsForOwner(destination).contains(segmentId) &&
               writeCh.getSegmentsForOwner(destination).contains(segmentId);
         boolean shouldIPush = readCh.locatePrimaryOwner(key).equals(rpcManager.getAddress());
         boolean destinationIsOwner = writeCh.isKeyLocalToNode(destination, key);
         return !isSegmentMoved && shouldIPush && destinationIsOwner;
      } else {
         return segments.contains(segmentId);
      }
   }

   @Override
   protected void sendEntries(boolean isLast) {
      if (segments == null) {
         List<StateChunk> chunks = new ArrayList<StateChunk>();
         for (List<InternalCacheEntry> entries : entriesBySegment.values()) {
            if (!entries.isEmpty() || isLast) {
               chunks.add(new StateChunk(-1, new ArrayList<InternalCacheEntry>(entries), isLast));
               entries.clear();
            }
         }

         sendChunks(chunks, isLast);         
      } else {
         super.sendEntries(isLast);
      }
      
   }
}
