package org.infinispan.util.concurrent.locks.containers.readwritelock;

import org.infinispan.util.concurrent.ConcurrentMapFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public abstract class AbstractPerEntryReadWriteLockContainer<L extends ReadWriteLock> extends AbstractReadWriteLockContainer<L> {
   //TODO: garbage collection of unused locks

   private final ConcurrentMap<Object, L> locks;

   protected AbstractPerEntryReadWriteLockContainer(int concurrencyLevel) {
      locks = ConcurrentMapFactory.makeConcurrentMap(16, concurrencyLevel);
   }

   @Override
   protected final L getLockFromMap(Object key) {
      L lock = locks.get(key);
      if(lock == null) {
         lock = newLock();
         L existing = locks.putIfAbsent(key, lock);
         if(existing != null) {
            lock = existing;
         }
      }
      return lock;
   }

   @Override
   public final L getReadWriteLock(Object key) {
      return locks.get(key);
   }

   @Override
   public final Lock getLock(Object key) {
      ReadWriteLock lock = getLockFromMap(key);      
      return lock.writeLock();
   }

   @Override
   public final Lock getSharedLock(Object key) {
      ReadWriteLock lock = getLockFromMap(key);
      return lock.readLock();
   }

   @Override
   public final int getNumLocksHeld() {
      int size = 0;
      for(L rwl : locks.values()) {
         if(isReadOrWriteLocked(rwl)) {
            size++;
         }
      }
      return size;
   }

   @Override
   public final int size() {
      return locks.size();
   }   

   @Override
   public final int getLockId(Object key) {
      return System.identityHashCode(getLock(key));
   }

   @Override
   public final void clear() {
      locks.clear();
   }

   @Override
   public final String toString() {
      return getClass().getSimpleName() +
            "{" +
            "locks=" + locks +
            '}';
   }

   protected abstract L newLock();
   protected abstract boolean isReadOrWriteLocked(L lock);
}
