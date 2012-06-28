package org.infinispan.reconfigurableprotocol.protocol;

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
 * @since 5.2
 */
public class TwoPhaseCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "2PC";

   @Override
   public String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public boolean switchTo(ReconfigurableProtocol protocol) {
      return PassiveReplicationCommitProtocol.UID.equals(protocol.getUniqueProtocolName());
   }

   @Override
   public void stopProtocol() throws InterruptedException {
      globalStopProtocol(false);
   }

   @Override
   public void bootProtocol() {
      //no-op
   }

   @Override
   public boolean canProcessOldTransaction(GlobalTransaction globalTransaction) {
      return PassiveReplicationCommitProtocol.UID.equals(globalTransaction.getReconfigurableProtocol().getUniqueProtocolName());
   }

   @Override
   public void bootstrapProtocol() {
      //no-op
   }

   @Override
   public EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      return buildDefaultInterceptorChain();
   }

   @Override
   public boolean use1PC(LocalTransaction localTransaction) {
      return configuration.transaction().use1PcForAutoCommitTransactions() && localTransaction.isImplicitTransaction();
   }

   @Override
   protected void internalHandleData(Object data, Address from) {
      //no-op
   }
}
