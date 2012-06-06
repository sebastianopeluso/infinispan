/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.transaction;

import java.util.*;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.ReadSetEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Base class for local and remote transaction. Impl note: The aggregated modification list and lookedUpEntries are not
 * instantiated here but in subclasses. This is done in order to take advantage of the fact that, for remote
 * transactions we already know the size of the modifications list at creation time.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 4.2
 */
public abstract class AbstractCacheTransaction implements CacheTransaction {

   protected final GlobalTransaction tx;
   private static Log log = LogFactory.getLog(AbstractCacheTransaction.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int INITIAL_LOCK_CAPACITY = 4;

   protected List<WriteCommand> modifications;
   protected HashMap<Object, CacheEntry> lookedUpEntries;
   protected Set<Object> affectedKeys = null;
   protected Set<Object> lockedKeys;
   protected Set<Object> backupKeyLocks = null;
   private boolean txComplete = false;
   protected volatile boolean prepared;
   private volatile boolean needToNotifyWaiters = false;
   final int viewId;

   private EntryVersionsMap updatedEntryVersions;

   protected Deque<ReadSetEntry> localReadSet;
   protected Deque<ReadSetEntry> remoteReadSet;
   protected BitSet alreadyRead = new BitSet();
   //protected BitSet realAlreadyRead = new BitSet();
   protected boolean alreadyReadOnNode=false;
   protected CacheEntry lastReadKey = null;
   protected VersionVC vectorClock;

   public AbstractCacheTransaction(GlobalTransaction tx, int viewId) {
      this.tx = tx;
      this.viewId = viewId;
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return tx;
   }

   @Override
   public List<WriteCommand> getModifications() {
      return modifications;
   }

   public void setModifications(WriteCommand[] modifications) {
      this.modifications = Arrays.asList(modifications);
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return lookedUpEntries;
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      if (lookedUpEntries == null) return null;
      return lookedUpEntries.get(key);
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      if (lookedUpEntries != null) lookedUpEntries.remove(key);
   }

   @Override
   public void clearLookedUpEntries() {
      lookedUpEntries = null;
   }

   @Override
   public boolean ownsLock(Object key) {
      return getLockedKeys().contains(key);
   }

   @Override
   public void notifyOnTransactionFinished() {
      if (trace) log.tracef("Transaction %s has completed, notifying listening threads.", tx);
      txComplete = true; //this one is cheap but does not guarantee visibility
      if (needToNotifyWaiters) {
         synchronized (this) {
            txComplete = true; //in this case we want to guarantee visibility to other threads
            this.notifyAll();
         }
      }
   }

   @Override
   public boolean waitForLockRelease(Object key, long lockAcquisitionTimeout) throws InterruptedException {
      if (txComplete) return true; //using an unsafe optimisation: if it's true, we for sure have the latest read of the value without needing memory barriers
      final boolean potentiallyLocked = hasLockOrIsLockBackup(key);
      if (trace) log.tracef("Transaction gtx=%s potentially locks key %s? %s", tx, key, potentiallyLocked);
      if (potentiallyLocked) {
         synchronized (this) {
            // Check again after acquiring a lock on the monitor that the transaction has completed.
            // If it has completed, all of its locks would have been released.
            needToNotifyWaiters = true;
            //The order in which these booleans are verified is critical as we take advantage of it to avoid otherwise needed locking
            if (txComplete) {
               needToNotifyWaiters = false;
               return true;
            }
            this.wait(lockAcquisitionTimeout);

            // Check again in case of spurious thread signalling
            return txComplete;
         }
      }
      return true;
   }

   @Override
   public int getViewId() {
      return viewId;
   }

   @Override
   public void addBackupLockForKey(Object key) {
      if (backupKeyLocks == null) backupKeyLocks = new HashSet<Object>(INITIAL_LOCK_CAPACITY);
      backupKeyLocks.add(key);
   }

   public void registerLockedKey(Object key) {
      if (lockedKeys == null) lockedKeys = new HashSet<Object>(INITIAL_LOCK_CAPACITY);
      if (trace) log.tracef("Registering locked key: %s", key);
      lockedKeys.add(key);
   }

   @Override
   public Set<Object> getLockedKeys() {
      return lockedKeys == null ? Collections.emptySet() : lockedKeys;
   }

   @Override
   public void clearLockedKeys() {
      if (trace) log.tracef("Clearing locked keys: %s", lockedKeys);
      lockedKeys = null;
   }

   private boolean hasLockOrIsLockBackup(Object key) {
      return (lockedKeys != null && lockedKeys.contains(key)) || (backupKeyLocks != null && backupKeyLocks.contains(key));
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys == null ? Collections.emptySet() : affectedKeys;
   }

   public void addAffectedKey(Object key) {
      initAffectedKeys();
      affectedKeys.add(key);
   }

   public void addAllAffectedKeys(Collection<Object> keys) {
      initAffectedKeys();
      affectedKeys.addAll(keys);
   }

   private void initAffectedKeys() {
      if (affectedKeys == null) affectedKeys = new HashSet<Object>(INITIAL_LOCK_CAPACITY);
   }

   @Override
   public EntryVersionsMap getUpdatedEntryVersions() {
      return updatedEntryVersions;
   }

   @Override
   public void setUpdatedEntryVersions(EntryVersionsMap updatedEntryVersions) {
      this.updatedEntryVersions = updatedEntryVersions;
   }
   
   @Override
   public void addReadKey(Object key) {
      // No-op
   }
   
   @Override
   public boolean keyRead(Object key) {
      return false;
   }

   @Override
   public void markPrepareSent() {
      //no-op
   }

   @Override
   public boolean wasPrepareSent() {
      return false;  // no-op
   }

   public void setAlreadyReadOnNode(boolean value){
      this.alreadyReadOnNode = value;
   }

   public boolean getAlreadyReadOnNode(){
      return this.alreadyReadOnNode;
   }

   public InternalMVCCEntry getLocalReadKey(Object key) {
      return localReadSet == null ? null : find(localReadSet, key);
   }

   public InternalMVCCEntry getRemoteReadKey(Object key) {
      return remoteReadSet == null ? null : find(remoteReadSet, key);
   }

   public Object[] getLocalReadSet() {
      return filterKeys(localReadSet);
   }

   public Object[] getRemoteReadSet() {
      return filterKeys(remoteReadSet);
   }

   public boolean hasAlreadyReadFrom(int idx) {
      return alreadyRead != null && alreadyRead.get(idx);
   }

   public void setAlreadyRead(int idx) {
      alreadyRead.set(idx);
   }

   public BitSet getAlreadyRead(){
      return (BitSet) alreadyRead.clone();
   }

   public void initVectorClock(VersionVCFactory versionVCFactory, VersionVC vc) {
      if(vectorClock == null) {
         vectorClock = versionVCFactory.createVersionVC();
      }
      
      vectorClock.setToMaximum(vc);
      int idx = versionVCFactory.getMyIndex();
      alreadyRead.set(idx);

      if(vectorClock.get(idx) == VersionVC.EMPTY_POSITION){
         vectorClock.set(versionVCFactory, idx, 0L);
      }

      this.alreadyReadOnNode = true; //because we don't want to search in the CommitLog. See MultiVersionDataContainer.get(...)
   }

   public void updateVectorClock(VersionVC other) {
      vectorClock.setToMaximum(other);
   }

   public void setVectorClockValueIn(VersionVCFactory versionVCFactory, int pos, long value){
      versionVCFactory.translateAndSet(vectorClock, pos, value);
   }

   public long getValueFrom(VersionVCFactory versionVCFactory, int idx) {
      return vectorClock != null ? versionVCFactory.translateAndGet(vectorClock,idx) : VersionVC.EMPTY_POSITION;
   }

   public VersionVC calculateVectorClockToRead(VersionVCFactory versionVCFactory) {
      VersionVC vc = versionVCFactory.createVisibleVersionVC(vectorClock, alreadyRead);
      /*
      VersionVC vc= versionVCFactory.createVersionVC();
      for(int i = 0; i < alreadyRead.length(); ++i) {
          if(alreadyRead.get(i)) {
              versionVCFactory.translateAndSet(vc,i, versionVCFactory.translateAndGet(vectorClock,i));
          }
      }
      */
      return vc;
   }

   public VersionVC getPrepareVectorClock() {
      try {
         return vectorClock.clone();
      } catch (CloneNotSupportedException e) {
         log.warnf("Exception caught while cloning Vector Clock. " + e.getMessage());
         log.trace(e);
         return null;
      }
   }

   public VersionVC getMinVersion() {
      return this.vectorClock;
   }

   private InternalMVCCEntry find(Deque<ReadSetEntry> readSetEntries, Object key){
      if(readSetEntries != null){
         Iterator<ReadSetEntry> itr = readSetEntries.descendingIterator();
         while(itr.hasNext()){
            ReadSetEntry entry = itr.next();

            if(entry.getKey() != null && entry.getKey().equals(key)){
               return entry.getIme();
            }
         }
      }
      return null;
   }

   private Object[] filterKeys(Deque<ReadSetEntry> readSetEntries){
      if(readSetEntries == null || readSetEntries.isEmpty()) {
         return new Object[0];
      }

      Object[] result = new Object[readSetEntries.size()];

      Iterator<ReadSetEntry> itr = readSetEntries.iterator();
      int i=0;
      while(itr.hasNext()){
         result[i] = itr.next().getKey();
         i++;
      }
      return result;
   }

   public abstract void addLocalReadKey(Object key, InternalMVCCEntry ime);

   public abstract void removeLocalReadKey(Object key);

   public abstract void removeRemoteReadKey(Object key);

   public abstract void addRemoteReadKey(Object key, InternalMVCCEntry ime);
}
