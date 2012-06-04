package org.infinispan.util.concurrent.locks.containers.readwritelock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public abstract class AbstractReadWriteLockContainer<L extends ReadWriteLock> implements ReadWriteLockContainer {

   protected final void safeReleaseWrite(L toRelease, Object lockOwner) {
      if (toRelease != null) {
         try {
            unlockWrite(toRelease, lockOwner);
         } catch (IllegalMonitorStateException imse) {
            // Perhaps the caller hadn't acquired the lock after all.
         }
      }
   }

   protected final void safeReleaseRead(L toRelease, Object lockOwner) {
      if (toRelease != null) {
         try {
            unlockRead(toRelease, lockOwner);
         } catch (IllegalMonitorStateException imse) {
            // Perhaps the caller hadn't acquired the lock after all.
         }
      }
   }

   @Override
   public final Lock acquireSharedLock(Object lockOwner, Object key, long timeout, TimeUnit unit) throws InterruptedException {
      L lock = getLockFromMap(key);
      boolean locked;
      try {
         locked = tryLockRead(lock, timeout, unit, lockOwner);
      } catch (InterruptedException ie) {
         safeReleaseRead(lock, lockOwner);
         throw ie;
      } catch (Throwable th) {
         safeReleaseRead(lock, lockOwner);
         locked = false;
      }
      return locked ? lock.readLock() : null;
   }

   @Override
   public final void releaseSharedLock(Object lockOwner, Object key) {
      L lock = getLockFromMap(key);
      safeReleaseRead(lock, lockOwner);
   }

   @Override
   public final Lock acquireLock(Object lockOwner, Object key, long timeout, TimeUnit unit) throws InterruptedException {
      L lock = getLockFromMap(key);
      boolean locked;
      try {
         locked = tryLockWrite(lock, timeout, unit, lockOwner);
      } catch (InterruptedException ie) {
         safeReleaseWrite(lock, lockOwner);
         throw ie;
      } catch (Throwable th) {
         safeReleaseWrite(lock, lockOwner);
         locked = false;
      }
      return locked ? lock.writeLock() : null;
   }

   @Override
   public final void releaseLock(Object lockOwner, Object key) {
      L lock = getLockFromMap(key);
      safeReleaseWrite(lock, lockOwner);
   }

   protected abstract void unlockWrite(L toRelease, Object lockOwner);
   protected abstract void unlockRead(L toRelease, Object lockOwner);

   protected abstract boolean tryLockWrite(L lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException;
   protected abstract boolean tryLockRead(L lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException;

   protected abstract L getLockFromMap(Object key);
}
