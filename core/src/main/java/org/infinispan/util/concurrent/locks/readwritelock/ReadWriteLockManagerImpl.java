package org.infinispan.util.concurrent.locks.readwritelock;

import org.infinispan.config.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.containers.readwritelock.ReadWriteLockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
@MBean(objectName = "ReadWriteLockManager", description = "Manager that handles MVCC locks (exclusive and shared) for entries")
public class ReadWriteLockManagerImpl implements ReadWriteLockManager {

   private static final String ANOTHER_THREAD = "(another thread)";

   protected Configuration configuration;
   protected ReadWriteLockContainer lockContainer;

   private static final Log log = LogFactory.getLog(ReadWriteLockManagerImpl.class);
   protected static final boolean trace = log.isTraceEnabled();

   @Stop
   public void stop() {
      lockContainer.clear();
   }

   public long getLockAcquisitionTimeout(InvocationContext ctx) {
      return ctx.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
            0 : configuration.getLockAcquisitionTimeout();
   }

   @Inject
   public void injectDependencies(Configuration configuration, ReadWriteLockContainer<?> lockContainer) {
      this.configuration = configuration;
      this.lockContainer = lockContainer;
   }

   @Override
   public boolean lockAndRecordShared(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      return lockAndRecordInternal(key, ctx, timeoutMillis, true);
   }

   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      return lockAndRecordInternal(key, ctx, timeoutMillis, false);
   }

   @Override
   public void unlockShared(Object key) {
      lockContainer.releaseSharedLock(ANOTHER_THREAD, key);
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      for (Object k : ctx.getLockedKeys()) {
         if (trace) log.tracef("Attempting to unlock %s", k);
         unlockBoth(ctx.getLockOwner(), k);
      }
      ctx.clearLockedKeys();
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      log.tracef("Attempting to unlock keys %s", lockedKeys);
      for (Object k : lockedKeys) {
         unlockBoth(lockOwner, k);
      }
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      return lockContainer.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      return lockContainer.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      if (lockContainer.isLocked(key)) {
         ReadWriteLock l = lockContainer.getReadWriteLock(key);

         if (l instanceof OwnableReentrantReadWriteLock) {
            return ((OwnableReentrantReadWriteLock) l).getOwner();
         } else {
            // cannot determine owner, JDK Reentrant locks only provide best-effort guesses.
            return ANOTHER_THREAD;
         }
      } else {
         return null;
      }
   }

   @Override
   public String printLockInfo() {
      return lockContainer.toString();
   }

   @Override
   public boolean possiblyLocked(CacheEntry entry) {
      return entry == null || entry.isChanged() || entry.isNull() || entry.isLockPlaceholder();
   }

   @Override
   public int getLockId(Object key) {
      return lockContainer.getLockId(key);
   }

   @Override
   public final boolean acquireLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      return acquireLock(ctx, key, -1);
   }

   @Override
   public final boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis) throws InterruptedException, TimeoutException {
      return acquireLockInternal(ctx, key, timeoutMillis, false);
   }

   @Override
   public final boolean acquireLockNoCheck(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      return acquireLockNoCheckInternal(ctx, key, false);
   }

   @Override
   public boolean acquireSharedLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      return acquireLockInternal(ctx, key, getLockAcquisitionTimeout(ctx), true);
   }

   private boolean acquireLockInternal(InvocationContext ctx, Object key, long timeoutMillis, boolean shared) throws InterruptedException, TimeoutException {
      // don't EVER use lockManager.isLocked() since with lock striping it may be the case that we hold the relevant
      // lock which may be shared with another key that we have a lock for already.
      // nothing wrong, just means that we fail to record the lock.  And that is a problem.
      // Better to check our records and lock again if necessary.
      if (!ctx.hasLockedKey(key) && !ctx.hasFlag(Flag.SKIP_LOCKING)) {
         return lockInternal(ctx, key, timeoutMillis < 0 ? getLockAcquisitionTimeout(ctx) : timeoutMillis, shared);
      } else {
         logLockNotAcquired(ctx);
      }
      return false;
   }

   private boolean acquireLockNoCheckInternal(InvocationContext ctx, Object key, boolean shared) throws InterruptedException, TimeoutException {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING)) {
         return lockInternal(ctx, key, getLockAcquisitionTimeout(ctx), shared);
      } else {
         logLockNotAcquired(ctx);
      }
      return false;
   }

   @Override
   public void addRegisterReadSample(long nanotime) {
      // TODO: Customise this generated block
   }

   private boolean lockInternal(InvocationContext ctx, Object key, long timeoutMillis, boolean shared) throws InterruptedException {
      boolean result = shared ? lockAndRecordShared(key, ctx, timeoutMillis) :
            lockAndRecord(key, ctx, timeoutMillis);
      if (result) {
         ctx.addLockedKey(key);
         return true;
      } else {
         Object owner = getOwner(key);
         // if lock cannot be acquired, expose the key itself, not the marshalled value
         if (key instanceof MarshalledValue) {
            key = ((MarshalledValue) key).get();
         }
         throw new TimeoutException("Unable to acquire lock after [" + Util.prettyPrintTime(getLockAcquisitionTimeout(ctx)) + "] on key [" + key + "] for requestor [" +
                                          ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
      }
   }

   private void logLockNotAcquired(InvocationContext ctx) {
      if (trace) {
         if (ctx.hasFlag(Flag.SKIP_LOCKING))
            log.trace("SKIP_LOCKING flag used!");
         else
            log.trace("Already own lock for entry");
      }
   }

   protected boolean lockAndRecordInternal(Object key, InvocationContext ctx, long timeoutMillis, boolean shared) throws InterruptedException {
      if (trace) log.tracef("Attempting to %s lock %s with acquisition timeout of %s millis", (shared ? "read" : "write"),
                            key, timeoutMillis);
      boolean result;

      if (shared) {
         result = lockContainer.acquireSharedLock(ctx.getLockOwner(), key, timeoutMillis, MILLISECONDS) != null;
      } else {
         result = lockContainer.acquireLock(ctx.getLockOwner(), key, timeoutMillis, MILLISECONDS) != null;
      }

      if (result) {
         if (trace) log.tracef("Successfully acquired lock %s!", key);
         return true;
      }

      // couldn't acquire lock!
      if (log.isDebugEnabled()) {
         log.debugf("Failed to acquire lock %s, owner is %s", key, getOwner(key));
         Object owner = ctx.getLockOwner();
         Set<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
         List<Object> lockedKeys = new ArrayList<Object>(entries.size());
         for (Map.Entry<Object, CacheEntry> e : entries) {
            Object lockedKey = e.getKey();
            if (ownsLock(lockedKey, owner)) {
               lockedKeys.add(lockedKey);
            }
         }
         log.debugf("This transaction (%s) already owned locks %s", owner, lockedKeys);
      }
      return false;
   }

   private void unlockBoth(Object lockOwner, Object key) {
      try{
         lockContainer.releaseSharedLock(lockOwner, key);
      } catch (IllegalMonitorStateException e) {
         //no-op
      }
      try{
         lockContainer.releaseLock(lockOwner, key);
      } catch (IllegalMonitorStateException e) {
         //no-op
      }
   }

   /*
   * ======================= JMX == STATS =============================
   */

   @ManagedAttribute(description = "The concurrency level that the MVCC Lock Manager has been configured with.")
   @Metric(displayName = "Concurrency level", dataType = DataType.TRAIT)
   public int getConcurrencyLevel() {
      return configuration.getConcurrencyLevel();
   }

   @ManagedAttribute(description = "The number of exclusive locks that are held.")
   @Metric(displayName = "Number of locks held")
   public int getNumberOfLocksHeld() {
      return lockContainer.getNumLocksHeld();
   }

   @ManagedAttribute(description = "The number of exclusive locks that are available.")
   @Metric(displayName = "Number of locks available")
   public int getNumberOfLocksAvailable() {
      return lockContainer.size() - lockContainer.getNumLocksHeld();
   }
}
