/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.ReadSetEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Context to be used for non transactional calls, both remote and local.
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 4.0
 */
public class NonTxInvocationContext extends AbstractInvocationContext {

   private static final int INITIAL_CAPACITY = 4;
   protected final Map<Object, CacheEntry> lookedUpEntries;

   protected Set<Object> lockedKeys;

   //for serializability
   private boolean readBasedOnVersion = false;
   private VersionVC versionVC = null;
   protected Deque<ReadSetEntry> readKeys = new LinkedList<ReadSetEntry>();
   private CacheEntry lastReadKey = null;
   private boolean alreadyReadOnNode = false;

   public NonTxInvocationContext(int numEntries, boolean local) {
      lookedUpEntries = new HashMap<Object, CacheEntry>(numEntries);
      setOriginLocal(local);
   }

   public NonTxInvocationContext() {
      lookedUpEntries = new HashMap<Object, CacheEntry>(INITIAL_CAPACITY);
   }

   @Override
   public CacheEntry lookupEntry(Object k) {
      return lookedUpEntries.get(k);
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      lookedUpEntries.remove(key);
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> newLookedUpEntries) {
      lookedUpEntries.putAll(newLookedUpEntries);
   }

   @Override
   public void clearLookedUpEntries() {
      lookedUpEntries.clear();
   }

   @Override
   @SuppressWarnings("unchecked")
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return (Map<Object, CacheEntry>)
            (lookedUpEntries == null ? Collections.emptyMap() : lookedUpEntries);
   }

   @Override
   public boolean isOriginLocal() {
      return isContextFlagSet(ContextFlag.ORIGIN_LOCAL);
   }

   public void setOriginLocal(boolean originLocal) {
      setContextFlag(ContextFlag.ORIGIN_LOCAL, originLocal);
   }

   @Override
   public boolean isInTxScope() {
      return false;
   }

   @Override
   public Object getLockOwner() {
      return Thread.currentThread();
   }

   @Override
   public void reset() {
      super.reset();
      clearLookedUpEntries();
      if (lockedKeys != null) lockedKeys.clear();
      resetMultiVersion();
   }

   @Override
   public NonTxInvocationContext clone() {
      NonTxInvocationContext dolly = (NonTxInvocationContext) super.clone();
      dolly.lookedUpEntries.putAll(lookedUpEntries);
      if(readKeys != null) {
         dolly.readKeys = new LinkedList<ReadSetEntry>(readKeys);
      }
      dolly.readBasedOnVersion = readBasedOnVersion;
      dolly.versionVC = versionVC;
      return dolly;
   }

   @Override
   public void addLockedKey(Object key) {
      if (lockedKeys == null) lockedKeys = new HashSet<Object>(INITIAL_CAPACITY);
      lockedKeys.add(key);
   }

   @Override
   public Set<Object> getLockedKeys() {
      return lockedKeys == null ? Collections.emptySet() : lockedKeys;
   }

   @Override
   public void clearLockedKeys() {
      lockedKeys = null;
   }

   @Override
   public boolean readBasedOnVersion() {
      return readBasedOnVersion;
   }

   @Override
   public void setReadBasedOnVersion(boolean value) {
      readBasedOnVersion = value;
   }

   @Override
   public void setVersionToRead(VersionVC version) {
      versionVC = version;
   }

   @Override
   public void addLocalReadKey(Object key, InternalMVCCEntry ime) {
      readKeys.addLast(new ReadSetEntry(key, ime));
   }

   @Override
   public void removeLocalReadKey(Object key) {
      if(readKeys != null){
         Iterator<ReadSetEntry> itr = readKeys.descendingIterator();
         while(itr.hasNext()){
            ReadSetEntry entry = itr.next();

            if(entry.getKey() != null && entry.getKey().equals(key)){
               itr.remove();
               break;
            }
         }
      }
   }

   @Override
   public void removeRemoteReadKey(Object key) {
      //no-op
   }

   @Override
   public void addRemoteReadKey(Object key, InternalMVCCEntry ime) {
      //no-op
   }

   @Override
   public InternalMVCCEntry getLocalReadKey(Object key) {
      return readKeys == null ? null : find(readKeys, key);
   }

   private InternalMVCCEntry find(Deque<ReadSetEntry> d, Object key){
      if(d != null){
         Iterator<ReadSetEntry> itr = d.descendingIterator();
         while(itr.hasNext()){
            ReadSetEntry entry = itr.next();

            if(entry.getKey() != null && entry.getKey().equals(key)){
               return entry.getIme();
            }
         }
      }
      return null;
   }

   @Override
   public InternalMVCCEntry getRemoteReadKey(Object key) {
      return null;
   }


   @Override
   public VersionVC calculateVersionToRead(VersionVCFactory versionVCFactory) {
      return versionVC;
   }

   @Override
   public void setAlreadyReadOnNode(boolean alreadyRead){
      this.alreadyReadOnNode = alreadyRead;
   }

   @Override
   public boolean getAlreadyReadOnNode(){
      return this.alreadyReadOnNode;
   }

   private void resetMultiVersion() {
      readBasedOnVersion = false;
      versionVC = null;
      readKeys.clear();
   }

   @Override
   public void setLastReadKey(CacheEntry entry){
      this.lastReadKey = entry;
   }

   @Override
   public CacheEntry getLastReadKey(){
      return this.lastReadKey;
   }

   @Override
   public void clearLastReadKey(){
      this.lastReadKey = null;
   }
}
