package org.infinispan.reconfigurableprotocol.protocol;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.interceptors.PassivationInterceptor;
import org.infinispan.interceptors.PassiveReplicationInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.EnumMap;

import static org.infinispan.interceptors.InterceptorChain.InterceptorType;

/**
 * Represents the switch protocol when Passive Replication is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PassiveReplicationCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "PB";
   private static final String MASTER_ACK = "_MASTER_ACK_";
   private static final String SWITCH_TO_MASTER_ACK = "_MASTER_ACK_2_";
   private boolean masterAckReceived = false;

   @Override
   public final String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public boolean canSwitchTo(ReconfigurableProtocol protocol) {
      return TwoPhaseCommitProtocol.UID.equals(protocol.getUniqueProtocolName());
   }

   @Override
   public void switchTo(ReconfigurableProtocol protocol) {
      manager.unsafeSwitch(protocol);
      new SendMasterAckThread().start();
   }

   @Override
   public final void stopProtocol() throws InterruptedException {
      if (isCoordinator()) {
         if (log.isDebugEnabled()) {
            log.debugf("[%s] Stop protocol in master for Passive Replication protocol. Wait until all local transactions " +
                             "are finished", Thread.currentThread().getName());
         }
         awaitUntilLocalTransactionsFinished();
         broadcastData(MASTER_ACK, false);
         if (log.isDebugEnabled()) {
            log.debugf("[%s] Ack sent to the slaves. Starting new epoch", Thread.currentThread().getName());
         }
      } else {
         if (log.isDebugEnabled()) {
            log.debugf("[%s] Stop protocol in slave for Passive Replication protocol. Wait for the master ack",
                       Thread.currentThread().getName());
         }
         synchronized (this) {
            while (!masterAckReceived) {
               this.wait();
            }
            masterAckReceived = false;
         }
         if (log.isDebugEnabled()) {
            log.debugf("[%s] Ack received from master. Starting new epoch", Thread.currentThread().getName());
         }
      }
      //this wait should return immediately, because we don't have any remote transactions pending...
      //it is just to be safe
      awaitUntilRemoteTransactionsFinished();
   }

   @Override
   public final void bootProtocol() {
      //no-op
   }

   @Override
   public void processTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet) {
      //no-op
   }

   @Override
   public void processOldTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet,
                                     ReconfigurableProtocol currentProtocol) {
      if (!TwoPhaseCommitProtocol.UID.equals(currentProtocol.getUniqueProtocolName())) {
         throwOldTxException();
      }
      //no-op
   }

   @Override
   public void processSpeculativeTransaction(GlobalTransaction globalTransaction, WriteCommand[] writeSet,
                                             ReconfigurableProtocol oldProtocol) {
      if (!TwoPhaseCommitProtocol.UID.equals(oldProtocol.getUniqueProtocolName())) {
         throwSpeculativeTxException();
      }
      //no-op
   }

   @Override
   public final void bootstrapProtocol() {
      //no-op
   }

   @Override
   public final EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      EnumMap<InterceptorType, CommandInterceptor> interceptors = buildDefaultInterceptorChain();

      //Custom interceptor after TxInterceptor
      interceptors.put(InterceptorType.CUSTOM_INTERCEPTOR_AFTER_TX_INTERCEPTOR,
                       createInterceptor(new PassivationInterceptor(), PassiveReplicationInterceptor.class));

      if (log.isTraceEnabled()) {
         log.tracef("Building interceptor chain for Passive Replication protocol %s", interceptors);
      }

      return interceptors;
   }

   @Override
   public final boolean use1PC(LocalTransaction localTransaction) {
      return !configuration.versioning().enabled() || configuration.transaction().useSynchronization();
   }

   @Override
   public boolean useTotalOrder() {
      return false;
   }

   @Override
   protected final void internalHandleData(Object data, Address from) {
      if (MASTER_ACK.equals(data)) {
         if (log.isDebugEnabled()) {
            log.debugf("Handle Master Ack message");
         }
         synchronized (this) {
            masterAckReceived = true;
            this.notifyAll();
         }
      } else if (SWITCH_TO_MASTER_ACK.equals(data)) {
         if (log.isDebugEnabled()) {
            log.debugf("Handle Switch To Master Ack message");
         }
         try {
            //just to be safe... we will not have remote transactions
            awaitUntilRemoteTransactionsFinished();
         } catch (InterruptedException e) {
            //ignore
         }
         manager.safeSwitch(null);
      }
   }

   private class SendMasterAckThread extends Thread {

      private SendMasterAckThread() {
         super("PB-Send-Ack-Thread");
      }

      @Override
      public void run() {
         try {
            awaitUntilLocalTransactionsFinished();
         } catch (InterruptedException e) {
            //interrupted
            return;
         }
         broadcastData(SWITCH_TO_MASTER_ACK, false);
         manager.safeSwitch(null);
      }
   }
}
