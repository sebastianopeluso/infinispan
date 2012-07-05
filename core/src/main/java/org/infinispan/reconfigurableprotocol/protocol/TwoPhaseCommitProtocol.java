package org.infinispan.reconfigurableprotocol.protocol;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.EnumMap;

import static org.infinispan.interceptors.InterceptorChain.InterceptorType;

/**
 * Represents the switch protocol when Two Phase Commit is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TwoPhaseCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "2PC";

   private static final String TO_UID = TotalOrderCommitProtocol.UID;
   private static final String PB_UID = PassiveReplicationCommitProtocol.UID;

   private static final String ACK = "_ACK_";

   private final AckCollector ackCollector = new AckCollector();

   @Override
   public final String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public boolean canSwitchTo(ReconfigurableProtocol protocol) {
      return PB_UID.equals(protocol.getUniqueProtocolName()) ||
            TO_UID.endsWith(protocol.getUniqueProtocolName());
   }

   @Override
   public final void switchTo(ReconfigurableProtocol protocol) {
      if (TO_UID.equals(protocol.getUniqueProtocolName())) {
         try {
            awaitUntilLocalTransactionsFinished();
         } catch (InterruptedException e) {
            //no-op
         }
      }
      manager.unsafeSwitch(protocol);
      new SendAckThread().start();
   }

   @Override
   public final void stopProtocol() throws InterruptedException {
      globalStopProtocol(false);
   }

   @Override
   public final void bootProtocol() {
      ackCollector.reset();
   }

   @Override
   public void processTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet) {
      //no-op
   }

   @Override
   public void processOldTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet,
                                     ReconfigurableProtocol currentProtocol) {
      if (PB_UID.equals(currentProtocol.getUniqueProtocolName())) {
         return;
      } else if (TO_UID.equals(currentProtocol.getUniqueProtocolName())) {
         if (writeSet == null) {
            //commit or rollback
            return;
         }
      }
      throwOldTxException();
   }

   @Override
   public void processSpeculativeTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet,
                                             ReconfigurableProtocol oldProtocol) {
      if (PB_UID.equals(oldProtocol.getUniqueProtocolName())) {
         return;
      }
      throwSpeculativeTxException();
   }

   @Override
   public final void bootstrapProtocol() {
      ackCollector.reset();
   }

   @Override
   public final EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      return buildDefaultInterceptorChain();
   }

   @Override
   public final boolean use1PC(LocalTransaction localTransaction) {
      return configuration.transaction().use1PcForAutoCommitTransactions() && localTransaction.isImplicitTransaction();
   }

   @Override
   public boolean useTotalOrder() {
      return false;
   }

   @Override
   protected final void internalHandleData(Object data, Address from) {
      if (ACK.equals(data)) {
         ackCollector.addAck(from);
      }
   }

   private class SendAckThread extends Thread {

      public SendAckThread() {
         super("2PC-Send-Ack-Thread");
      }

      @Override
      public void run() {
         broadcastData(ACK, false);
         try {
            ackCollector.awaitAllAck();
            awaitUntilRemoteTransactionsFinished();
         } catch (InterruptedException e) {
            //no-op
         }
         manager.safeSwitch(null);
         ackCollector.reset();
      }
   }
}
