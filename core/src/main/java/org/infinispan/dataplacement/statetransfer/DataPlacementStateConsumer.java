package org.infinispan.dataplacement.statetransfer;

import org.infinispan.dataplacement.ClusterObjectLookup;
import org.infinispan.dataplacement.ch.DataPlacementConsistentHash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.InboundTransferTask;
import org.infinispan.statetransfer.StateConsumerImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class DataPlacementStateConsumer extends StateConsumerImpl {

   @Override
   protected void afterStateTransferStarted(ConsistentHash oldCh, ConsistentHash newCh) {
      super.afterStateTransferStarted(oldCh, newCh);
      if (oldCh instanceof DataPlacementConsistentHash && newCh instanceof DataPlacementConsistentHash) {
         List<ClusterObjectLookup> oldMappings = ((DataPlacementConsistentHash) oldCh).getClusterObjectLookupList();
         List<ClusterObjectLookup> newMappings = ((DataPlacementConsistentHash) newCh).getClusterObjectLookupList();

         if (oldMappings.equals(newMappings)) {
            return;
         }
         //we have a new mapping... trigger data placement state transfer
         synchronized (this) {
            for (Address source : cacheTopology.getMembers()) {
               if (source.equals(rpcManager.getAddress())) {
                  continue;
               }
               InboundTransferTask inboundTransfer = new InboundTransferTask(null, source, cacheTopology.getTopologyId(),
                                                                             this, rpcManager, commandsFactory,
                                                                             timeout, cacheName);
               List<InboundTransferTask> inboundTransfers = transfersBySource.get(inboundTransfer.getSource());
               if (inboundTransfers == null) {
                  inboundTransfers = new ArrayList<InboundTransferTask>();
                  transfersBySource.put(inboundTransfer.getSource(), inboundTransfers);
               }
               inboundTransfers.add(inboundTransfer);
               taskQueue.add(inboundTransfer);
            }
         }
      }

   }
}
