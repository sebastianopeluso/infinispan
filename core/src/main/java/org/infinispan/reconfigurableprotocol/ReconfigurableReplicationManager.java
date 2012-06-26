package org.infinispan.reconfigurableprotocol;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;import org.infinispan.reconfigurableprotocol.exception.NoSuchReconfigurableProtocolException;
import org.infinispan.reconfigurableprotocol.manager.EpochManager;
import org.infinispan.reconfigurableprotocol.manager.ProtocolManager;
import org.infinispan.transaction.xa.GlobalTransaction;

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
      protocolManager.change(newProtocol);
      epochManager.incrementEpoch();
      //TODO mark switch finished
   }

   public final void notifyLocalTransaction(GlobalTransaction globalTransaction) {
      //TODO check switch in progress
      long epoch = epochManager.getEpoch();
      ReconfigurableProtocol actual = protocolManager.getActual();
      short protocolId = registry.getReconfigurableProtocolById(actual);

      globalTransaction.setEpochId(epoch);
      globalTransaction.setProtocolId(protocolId);
      globalTransaction.setReconfigurableProtocol(actual);           
   }

   public final void notifyRemoteTransaction(GlobalTransaction globalTransaction) {
      long txEpoch = globalTransaction.getEpochId();
      long epoch = epochManager.getEpoch();
      ReconfigurableProtocol protocol = registry.getReconfigurableProtocol(globalTransaction.getProtocolId());
      globalTransaction.setReconfigurableProtocol(protocol);
      
      if (txEpoch < epoch) {
         ReconfigurableProtocol actual = protocolManager.getActual();
         //TODO modifications
         if (!actual.canProcessTransactionFromPreviousEpoch(globalTransaction, new WriteCommand[0])) {
            //TODO throw new exception
         }         
      } else if (txEpoch > epoch) {
         //new epoch transaction... block         
         //TODO block 
      }            
   }   
}