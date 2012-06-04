package org.infinispan.util.concurrent.locks.containers.readwritelock;

import org.infinispan.util.concurrent.locks.readwritelock.OwnableReentrantReadWriteLock;

import java.util.concurrent.TimeUnit;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class OwnableReentrantPerEntryReadWriteLockContainer extends AbstractPerEntryReadWriteLockContainer<OwnableReentrantReadWriteLock> {

   public OwnableReentrantPerEntryReadWriteLockContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   @Override
   protected final OwnableReentrantReadWriteLock newLock() {
      return new OwnableReentrantReadWriteLock();
   }

   @Override
   protected final boolean isReadOrWriteLocked(OwnableReentrantReadWriteLock lock) {
      return lock != null && lock.isReadOrWriteLocked();
   }

   @Override
   protected final void unlockWrite(OwnableReentrantReadWriteLock toRelease, Object lockOwner) {
      toRelease.unlockWrite(lockOwner);
   }

   @Override
   protected final void unlockRead(OwnableReentrantReadWriteLock toRelease, Object lockOwner) {
      toRelease.unlockRead(lockOwner);
   }

   @Override
   protected final boolean tryLockWrite(OwnableReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLockWrite(lockOwner, timeout, unit);
   }

   @Override
   protected final boolean tryLockRead(OwnableReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLockRead(lockOwner, timeout, unit);
   }

   @Override
   public final boolean ownsReadOrWriteLock(Object owner, Object key) {
      OwnableReentrantReadWriteLock l = getReadWriteLock(key);
      return l != null && l.hasReadOrWriteLock(owner);
   }

   @Override
   public final boolean ownsLock(Object key, Object owner) {
      OwnableReentrantReadWriteLock l = getReadWriteLock(key);
      return owner != null && owner.equals(l.getOwner());
   }

   @Override
   public final boolean isLocked(Object key) {
      OwnableReentrantReadWriteLock l = getReadWriteLock(key);
      return l != null && l.isWriteLock();
   }
}
