package org.infinispan.util.concurrent.locks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OwnableReentrantReadWriteLock extends OwnableReentrantLock {   

   private transient final Map<Object, AtomicInteger> readCounters = new HashMap<Object, AtomicInteger>();

   public final boolean tryShareLock(Object requestor, long time, TimeUnit unit) throws InterruptedException {
      setCurrentRequestor(requestor);
      try {
         return tryAcquireSharedNanos(1, unit.toNanos(time));
      } finally {
         unsetCurrentRequestor();
      }
   }

   public final void unlockShare(Object requestor) {
      setCurrentRequestor(requestor);
      try {
         releaseShared(1);
      } catch (IllegalMonitorStateException imse) {
         // ignore?
      } finally {
         unsetCurrentRequestor();
      }
   }
   
   public final boolean ownsShareLock(Object owner) {
      synchronized (readCounters) {
         return readCounters.containsKey(owner);
      }
   }
   
   public final boolean isShareLocked() {
      return getState() < 0;
   }

   @Override
   protected int tryAcquireShared(int i) {
      Object requestor = currentRequestor();
      while (true) {
         int state = getState();
         if (state <= 0 && compareAndSetState(state, state - 1)) {
            incrementRead(requestor);
            return 1;
         } else if (state > 0) {
            return requestor.equals(getOwner()) ? 0 : -1 ;
         }
      }
   }

   @Override
   protected boolean tryReleaseShared(int i) {
      if (!decrementRead(currentRequestor())) {
         return false;
      }
      while (true) {
         int state = getState();
         if (compareAndSetState(state, state + 1)) {
            return true;
         }
      }
   }        

   @Override
   protected void resetState() {
      super.resetState();
      readCounters.clear();
   }

   private void incrementRead(Object owner) {
      synchronized (readCounters) {
         AtomicInteger counter = readCounters.get(owner);
         if (counter == null) {
            readCounters.put(owner, new AtomicInteger(1));
         } else {
            counter.incrementAndGet();
         }
      }
   }
   
   private boolean decrementRead(Object owner) {
      synchronized (readCounters) {
         AtomicInteger counter = readCounters.get(owner);
         if (counter == null) {
            return false;
         }
         if (counter.decrementAndGet() == 0) {
            readCounters.remove(owner);
         }
         return true;
      }
   }
}
