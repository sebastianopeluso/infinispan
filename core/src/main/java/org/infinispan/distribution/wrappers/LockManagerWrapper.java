package org.infinispan.distribution.wrappers;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.Collection;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
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
            } else {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
            }
            return true;
         }
      }
      return false;
   }

   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      return actual.lockAndRecord(key, ctx, timeoutMillis);
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      actual.unlock(lockedKeys, lockOwner);
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      actual.unlockAll(ctx);
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      return actual.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      return actual.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      return actual.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      return actual.printLockInfo();
   }

   @Override
   public boolean possiblyLocked(CacheEntry entry) {
      return actual.possiblyLocked(entry);
   }

   @Override
   public int getNumberOfLocksHeld() {
      return actual.getNumberOfLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      return actual.getLockId(key);
   }

   @Override
   public boolean acquireLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      long lockingTime = 0;
      boolean locked,
            experiencedContention = false,
            txScope;

      if(txScope = ctx.isInTxScope()){
         experiencedContention = this.updateContentionStats(key,(TxInvocationContext)ctx);
         lockingTime = System.nanoTime();
      }
      try{
         locked = actual.acquireLock(ctx, key);  //this returns false if you already have acquired the lock previously
      }
      catch(TimeoutException e){
         StreamLibContainer.getInstance().addLockInformation(key, experiencedContention, true);
         throw e;
      }
      catch(InterruptedException e){
         StreamLibContainer.getInstance().addLockInformation(key, experiencedContention, true);
         throw e;
      }

      StreamLibContainer.getInstance().addLockInformation(key, experiencedContention, false);

      if(txScope && experiencedContention && locked){
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
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis) throws InterruptedException,
                                                                                            TimeoutException {
      return actual.acquireLock(ctx, key, timeoutMillis);
   }

   @Override
   public boolean acquireLockNoCheck(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      return actual.acquireLockNoCheck(ctx, key);
   }
}
