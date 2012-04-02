package org.infinispan.distribution.wrappers;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.Collection;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
public class LockManagerWrapper implements LockManager {


   private final LockManager actual;

   public LockManagerWrapper(LockManager actual) {
      this.actual = actual;
   }

   private boolean updateContentionStats(Object key, TxInvocationContext tctx){
      GlobalTransaction holder = (GlobalTransaction)getOwner(key);
      if(holder!=null){
         GlobalTransaction me = tctx.getGlobalTransaction();
         if(holder!=me){
            if(holder.isRemote()){
               TransactionsStatisticsRegistry.incrementValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
            }
            else
               TransactionsStatisticsRegistry.incrementValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
         }
         return true;
      }
      return false;

   }

   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      System.out.println("LockManagerWrapper.lockAndRecord");


      return actual.lockAndRecord(key, ctx, timeoutMillis);


   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      System.out.println("LockManagerWrapper.unlock");
      actual.unlock(lockedKeys, lockOwner);
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      System.out.println("LockManagerWrapper.unlockAll");
      actual.unlockAll(ctx);
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      System.out.println("LockManagerWrapper.ownsLock");
      return actual.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      System.out.println("LockManagerWrapper.isLocked");
      return actual.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      System.out.println("LockManagerWrapper.getOwner");
      return actual.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      System.out.println("LockManagerWrapper.printLockInfo");
      return actual.printLockInfo();
   }

   @Override
   public boolean possiblyLocked(CacheEntry entry) {
      System.out.println("LockManagerWrapper.possiblyLocked");
      return actual.possiblyLocked(entry);
   }

   @Override
   public int getNumberOfLocksHeld() {
      System.out.println("LockManagerWrapper.getNumberOfLocksHeld");
      return actual.getNumberOfLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      System.out.println("LockManagerWrapper.getLockId");
      return actual.getLockId(key);
   }

   @Override
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      System.out.println("LockManagerWrapper.acquireLock");

      long lockingTime = 0;
      boolean experiencedContention = false;
      boolean txScope = ctx.isInTxScope();

      if(txScope){
         experiencedContention = this.updateContentionStats(key,(TxInvocationContext)ctx);
         lockingTime = System.nanoTime();
      }

      boolean locked = actual.acquireLock(ctx, key, timeoutMillis, skipLocking);

      if(txScope && experiencedContention){
         lockingTime = System.nanoTime() - lockingTime;
         TransactionsStatisticsRegistry.addValue(IspnStats.LOCK_WAITING_TIME,lockingTime);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_WAITED_FOR_LOCKS);
      }
      if(locked){
         TransactionsStatisticsRegistry.addTakenLock(key); //Idempotent
      }
      return locked;
   }

   @Override
   public boolean acquireLockNoCheck(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      System.out.println("LockManagerWrapper.acquireLockNoCheck");

      long lockingTime = 0;
      boolean experiencedContention = false;
      boolean txScope = ctx.isInTxScope();

      if(txScope){
         experiencedContention = this.updateContentionStats(key,(TxInvocationContext)ctx);
         lockingTime = System.nanoTime();
      }

      boolean locked = actual.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking);

      if(txScope && experiencedContention){
         lockingTime = System.nanoTime() - lockingTime;
         TransactionsStatisticsRegistry.addValue(IspnStats.LOCK_WAITING_TIME,lockingTime);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_WAITED_FOR_LOCKS);
      }
      if(locked){
         TransactionsStatisticsRegistry.addTakenLock(key); //Idempotent
      }
      return locked;
   }   
}
