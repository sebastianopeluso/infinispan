package org.infinispan.util.concurrent.locks.containers.readwritelock;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public abstract class AbstractStripedReadWriteLockContainer<L extends ReadWriteLock> extends AbstractReadWriteLockContainer<L> {

   private int lockSegmentMask;
   private int lockSegmentShift;

   protected final int calculateNumberOfSegments(int concurrencyLevel) {
      int tempLockSegShift = 0;
      int numLocks = 1;
      while (numLocks < concurrencyLevel) {
         ++tempLockSegShift;
         numLocks <<= 1;
      }
      lockSegmentShift = 32 - tempLockSegShift;
      lockSegmentMask = numLocks - 1;
      return numLocks;
   }

   protected final int hashToIndex(Object object) {
      return (hash(object) >>> lockSegmentShift) & lockSegmentMask;
   }

   /**
    * Returns a hash code for non-null Object x. Uses the same hash code spreader as most other java.util hash tables,
    * except that this uses the string representation of the object passed in.
    *
    * @param object the object serving as a key
    * @return the hash code
    */
   public static int hash(Object object) {
      int h = object.hashCode();
      h += ~(h << 9);
      h ^= (h >>> 14);
      h += (h << 4);
      h ^= (h >>> 10);
      return h;

   }

   @Override
   public int getLockId(Object key) {
      return hashToIndex(key);
   }

   protected abstract void initLocks(int numLocks);
}
