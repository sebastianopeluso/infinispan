package org.infinispan.util.concurrent.locks.containers.readwritelock;

import org.infinispan.util.concurrent.locks.containers.LockContainer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public interface ReadWriteLockContainer<L extends ReadWriteLock> extends LockContainer {

   /**
    * obtains the read lock
    * @param key the key
    * @return the corresponding read lock
    */
   Lock getSharedLock(Object key);

   /**
    * attempts to acquire the read lock with a certain timeout
    * @param key object to acquire lock on
    * @param timeout Time after which the lock acquisition will fail
    * @param unit Time unit of the given timeout
    * @return the lock if it is acquired, or null otherwise
    * @throws InterruptedException If the lock acquisition was interrupted
    */
   Lock acquireSharedLock(Object lockOwner, Object key, long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * release the lock on the given key
    * @param key object in which the read lock is unlock
    */
   void releaseSharedLock(Object lockOwner, Object key);

   void clear();

   L getReadWriteLock(Object key);

   boolean ownsReadOrWriteLock(Object owner, Object key);
}
