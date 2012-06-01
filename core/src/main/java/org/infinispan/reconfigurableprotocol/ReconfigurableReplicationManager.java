package org.infinispan.reconfigurableprotocol;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;import org.infinispan.reconfigurableprotocol.exception.NoSuchReconfigurableProtocolException;
import org.infinispan.reconfigurableprotocol.manager.EpochManager;
import org.infinispan.reconfigurableprotocol.manager.ProtocolManager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class ReconfigurableReplicationManager {

   private ReconfigurableProtocolRegistry registry;
   private final EpochManager epochManager = new EpochManager();
   private final ProtocolManager protocolManager = new ProtocolManager();

   @Inject
   public final void inject(ReconfigurableProtocolRegistry registry) {
      this.registry = registry;
   }

   @Start
   public final void bootstrapManager() {
      //TODO
      //create protocol for 2PC, TO and PB
      //register them in registry
      //check the actual protocol
   }

   public final void switchTo(short protocolId) throws NoSuchReconfigurableProtocolException {
      ReconfigurableProtocol newProtocol = registry.getReconfigurableProtocol(protocolId);
      if (newProtocol == null) {
         throw new NoSuchReconfigurableProtocolException("Protocol ID " + protocolId + " not found");
      } else if (protocolManager.isActual(newProtocol)) {
         return; //nothing to do
      }

      //TODO mark switch in progress
      ReconfigurableProtocol actual = protocolManager.getActual();
      if (!actual.switchTo(newProtocol)) {
         actual.stopProtocol();
         newProtocol.bootProtocol();
      }
      epochManager.incrementEpoch();
      //TODO mark switch finished
   }

   public final ProtocolInfo notifyLocalTransactionWantsToFinish(/*some transaction*/) {
      //TODO check switch in progress
      long epoch = epochManager.getEpoch();
      ReconfigurableProtocol actual = protocolManager.getActual();
      long protocolId = registry.getReconfigurableProtocolById(actual);

      return new ProtocolInfo(epoch, protocolId, actual);
   }

   public static class ProtocolInfo {
      private final long epoch;
      private final long protocolId;
      private final ReconfigurableProtocol actualProtocol;

      public ProtocolInfo(long epoch, long protocolId, ReconfigurableProtocol actualProtocol) {
         this.epoch = epoch;
         this.protocolId = protocolId;
         this.actualProtocol = actualProtocol;
      }

      public final long getEpoch() {
         return epoch;
      }

      public final long getProtocolId() {
         return protocolId;
      }

      public final ReconfigurableProtocol getActualProtocol() {
         return actualProtocol;
      }
   }
}