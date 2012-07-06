package org.infinispan.reconfigurableprotocol.component;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.LockInfo;
import org.infinispan.statetransfer.StateTransferManager;

import java.util.Collection;

/**
 * Delegates the method invocations for the correct instance depending of the protocol, for the StateTransferManager
 * component
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StateTransferManagerDelegate extends AbstractProtocolDependentComponent<StateTransferManager>
      implements StateTransferManager {

   @Override
   public boolean isJoinComplete() {
      return get().isJoinComplete();
   }

   @Override
   public void waitForJoinToComplete() throws InterruptedException {
      get().waitForJoinToComplete();
   }

   @Override
   public boolean hasJoinStarted() {
      return get().hasJoinStarted();
   }

   @Override
   public void waitForJoinToStart() throws InterruptedException {
      get().waitForJoinToStart();
   }

   @Override
   public boolean isStateTransferInProgress() {
      return get().isStateTransferInProgress();
   }

   @Override
   public void waitForStateTransferToComplete() throws InterruptedException {
      get().waitForStateTransferToComplete();
   }

   @Override
   public void applyState(Collection<InternalCacheEntry> state, Address sender, int viewId) throws InterruptedException {
      get().applyState(state, sender, viewId);
   }

   @Override
   public void applyLocks(Collection<LockInfo> locks, Address sender, int viewId) throws InterruptedException {
      get().applyLocks(locks, sender, viewId);
   }

   @Override
   public boolean isLocationInDoubt(Object key) {
      return get().isLocationInDoubt(key);
   }
}
