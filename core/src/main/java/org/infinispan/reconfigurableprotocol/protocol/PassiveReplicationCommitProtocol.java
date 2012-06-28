package org.infinispan.reconfigurableprotocol.protocol;

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
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class PassiveReplicationCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "PB";
   private static final String MASTER_ACK = "_MASTER_ACK_";
   private boolean masterAckReceived = false;

   @Override
   public String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public boolean switchTo(ReconfigurableProtocol protocol) {
      return TwoPhaseCommitProtocol.UID.equals(protocol.getUniqueProtocolName());
   }

   @Override
   public void stopProtocol() throws InterruptedException {
      if (isCoordinator()) {
         awaitUntilLocalTransactionsFinished();
         broadcastData(MASTER_ACK, false);
      } else {
         synchronized (this) {
            while (!masterAckReceived) {
               this.wait();
            }
            masterAckReceived = false;
         }
      }
      awaitUntilRemoteTransactionsFinished();
   }

   @Override
   public void bootProtocol() {
      //no-op
   }

   @Override
   public boolean canProcessOldTransaction(GlobalTransaction globalTransaction) {
      return TwoPhaseCommitProtocol.UID.equals(globalTransaction.getReconfigurableProtocol().getUniqueProtocolName());
   }

   @Override
   public void bootstrapProtocol() {
      //no-op
   }

   @Override
   public EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      EnumMap<InterceptorType, CommandInterceptor> interceptors = buildDefaultInterceptorChain();

      //Custom interceptor after TxInterceptor
      interceptors.put(InterceptorType.CUSTOM_INTERCEPTOR_AFTER_TX_INTERCEPTOR,
                       createInterceptor(new PassivationInterceptor(), PassiveReplicationInterceptor.class));

      return interceptors;
   }

   @Override
   public boolean use1PC(LocalTransaction localTransaction) {
      return !configuration.versioning().enabled() || configuration.transaction().useSynchronization();
   }

   @Override
   protected void internalHandleData(Object data, Address from) {
      if (MASTER_ACK.equals(data)) {
         synchronized (this) {
            masterAckReceived = true;
            this.notifyAll();
         }
      }
   }
}
