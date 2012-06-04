package org.infinispan.util.concurrent.locks.containers.readwritelock;

import org.infinispan.util.concurrent.locks.readwritelock.OwnableReentrantReadWriteLock;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class OwnableReentrantStripedReadWriteLockContainer extends AbstractStripedReadWriteLockContainer<OwnableReentrantReadWriteLock> {

   private OwnableReentrantReadWriteLock[] sharedLocks;

   public OwnableReentrantStripedReadWriteLockContainer(int concurrencyLevel) {
      initLocks(calculateNumberOfSegments(concurrencyLevel));
   }

   @Override
   protected final void initLocks(int numLocks) {
      sharedLocks = new OwnableReentrantReadWriteLock[numLocks];
      for(int i = 0; i < numLocks; ++i) {
         sharedLocks[i] = new OwnableReentrantReadWriteLock();
      }
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
   protected final OwnableReentrantReadWriteLock getLockFromMap(Object key) {
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
   public final OwnableReentrantReadWriteLock getReadWriteLock(Object key) {
      return getLockFromMap(key);
   }

   @Override
   public final boolean ownsReadOrWriteLock(Object owner, Object key) {
      OwnableReentrantReadWriteLock l = getLockFromMap(key);
      return owner != null && l.hasReadOrWriteLock(owner);
   }

   @Override
   public final boolean ownsLock(Object key, Object owner) {
      OwnableReentrantReadWriteLock l = getLockFromMap(key);
      return owner != null && owner.equals(l.getOwner());
   }

   @Override
   public final boolean isLocked(Object key) {
      return getLockFromMap(key).isWriteLock();
   }

   @Override
   public final Lock getLock(Object key) {
      return getLockFromMap(key).writeLock();
   }

   @Override
   public final int getNumLocksHeld() {
      int size = 0;
      for(OwnableReentrantReadWriteLock l : sharedLocks) {
         if(l.isReadOrWriteLocked()) {
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
      return "OwnableReentrantStripedReadWriteLockContainer{" +
            "sharedLocks=" + (sharedLocks == null ? null : Arrays.asList(sharedLocks)) +
            '}';
   }

}
