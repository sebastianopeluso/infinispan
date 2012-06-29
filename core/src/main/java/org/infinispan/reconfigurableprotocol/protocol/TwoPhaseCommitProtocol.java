package org.infinispan.reconfigurableprotocol.protocol;

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

   @Override
   public final String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public final boolean switchTo(ReconfigurableProtocol protocol) {
      return PassiveReplicationCommitProtocol.UID.equals(protocol.getUniqueProtocolName());
   }

   @Override
   public final void stopProtocol() throws InterruptedException {
      globalStopProtocol(false);
   }

   @Override
   public final void bootProtocol() {
      //no-op
   }

   @Override
   public final boolean canProcessOldTransaction(GlobalTransaction globalTransaction) {
      return PassiveReplicationCommitProtocol.UID.equals(globalTransaction.getReconfigurableProtocol().getUniqueProtocolName());
   }

   @Override
   public final void bootstrapProtocol() {
      //no-op
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
   protected final void internalHandleData(Object data, Address from) {
      //no-op
   }
}
