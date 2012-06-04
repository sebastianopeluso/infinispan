package org.infinispan.util.concurrent.locks.containers.readwritelock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class ReentrantPerEntryReadWriteLockContainer extends AbstractPerEntryReadWriteLockContainer<ReentrantReadWriteLock> {


   public ReentrantPerEntryReadWriteLockContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   @Override
   protected final ReentrantReadWriteLock newLock() {
      return new ReentrantReadWriteLock();
   }

   @Override
   protected final boolean isReadOrWriteLocked(ReentrantReadWriteLock lock) {
      return lock != null && (lock.getReadLockCount() > 0 || lock.isWriteLocked());
   }

   @Override
   protected final void unlockWrite(ReentrantReadWriteLock toRelease, Object lockOwner) {
      toRelease.writeLock().unlock();
   }

   @Override
   protected final void unlockRead(ReentrantReadWriteLock toRelease, Object lockOwner) {
      toRelease.readLock().unlock();
   }

   @Override
   protected final boolean tryLockWrite(ReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.writeLock().tryLock(timeout, unit);
   }

   @Override
   protected final boolean tryLockRead(ReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.readLock().tryLock(timeout, unit);
   }

   @Override
   public final boolean ownsReadOrWriteLock(Object owner, Object key) {
      ReentrantReadWriteLock l = getReadWriteLock(key);
      return l != null && (l.isWriteLockedByCurrentThread() || l.getReadLockCount() != 0);
   }

   @Override
   public final boolean ownsLock(Object key, Object owner) {
      ReentrantReadWriteLock l = getReadWriteLock(key);
      return l != null && l.isWriteLockedByCurrentThread();
   }

   @Override
   public final boolean isLocked(Object key) {
      ReentrantReadWriteLock l = getReadWriteLock(key);
      return l != null && l.isWriteLocked();
   }
}
