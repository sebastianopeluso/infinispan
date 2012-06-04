package org.infinispan.util.concurrent.locks.containers.readwritelock;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class ReentrantStripedReadWriteLockContainer extends AbstractStripedReadWriteLockContainer<ReentrantReadWriteLock> {

   private ReentrantReadWriteLock[] sharedLocks;

   public ReentrantStripedReadWriteLockContainer(int concurrencyLevel) {
      initLocks(calculateNumberOfSegments(concurrencyLevel));
   }

   @Override
   protected final void initLocks(int numLocks) {
      sharedLocks = new ReentrantReadWriteLock[numLocks];
      for(int i = 0; i < numLocks; ++i) {
         sharedLocks[i] = new ReentrantReadWriteLock();
      }
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
   protected final ReentrantReadWriteLock getLockFromMap(Object key) {
      return sharedLocks[hashToIndex(key)];
   }

   @Override
   public final Lock getSharedLock(Object key) {
      return getLockFromMap(key).readLock();
   }

   @Override
   public final void clear() {
      sharedLocks = null;
   }

   @Override
   public final ReentrantReadWriteLock getReadWriteLock(Object key) {
      return getLockFromMap(key);
   }

   @Override
   public final boolean ownsReadOrWriteLock(Object owner, Object key) {
      ReentrantReadWriteLock l = getLockFromMap(key);
      return l != null && (l.isWriteLockedByCurrentThread() || l.getReadLockCount() != 0);
   }

   @Override
   public final boolean ownsLock(Object key, Object owner) {
      ReentrantReadWriteLock l = getLockFromMap(key);
      return l.isWriteLockedByCurrentThread();
   }

   @Override
   public final boolean isLocked(Object key) {
      ReentrantReadWriteLock l = getLockFromMap(key);
      return l.isWriteLocked();
   }

   @Override
   public Lock getLock(Object key) {
      return getLockFromMap(key).writeLock();
   }

   @Override
   public final int getNumLocksHeld() {
      int size = 0;
      for (ReentrantReadWriteLock l : sharedLocks) {
         if (l.getReadLockCount() > 0 || l.isWriteLocked()) {
            size++;
         }
      }
      return size;
   }

   @Override
   public final int size() {
      return sharedLocks.length;
   }

   @Override
   public final String toString() {
      return "ReentrantStripedReadWriteLockContainer{" +
            "sharedLocks=" + (sharedLocks == null ? null : Arrays.asList(sharedLocks)) +
            '}';
   }
}
