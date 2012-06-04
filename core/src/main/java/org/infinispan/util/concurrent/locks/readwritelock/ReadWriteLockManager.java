package org.infinispan.util.concurrent.locks.readwritelock;

import org.infinispan.context.InvocationContext;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public interface ReadWriteLockManager extends LockManager {

   /**
    * Acquires the shared (read) lock on a specific entry in the cache.  This method will try for a period of time and
    * give up if it is unable to acquire the required lock. The period of time is specified in {@link
    * org.infinispan.config.Configuration#getLockAcquisitionTimeout()}.
    *
    * @param key key to lock
    * @param ctx invocation context associated with this invocation
    * @return true if the lock was acquired, false otherwise.
    * @throws InterruptedException if interrupted
    */
   boolean lockAndRecordShared(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException;

   /**
    * Releases the shared (read) lock for the key passed in
    *
    * @param key key to unlock
    */
   void unlockShared(Object key);

   /**
    * return the lock acquisition timeout for the given context
    * @param ctx the context
    * @return the lock acquisition timeout
    */
   long getLockAcquisitionTimeout(InvocationContext ctx);

   /**
    * 
    * @param ctx
    * @param key
    * @return
    * @throws InterruptedException
    * @throws TimeoutException
    */
   boolean acquireSharedLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException;

   /**
    * 
    * @param nanotime
    */
   public void addRegisterReadSample(long nanotime);
}
