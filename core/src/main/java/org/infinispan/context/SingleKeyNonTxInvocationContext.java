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

package org.infinispan.context;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.AbstractInvocationContext;
import org.infinispan.mvcc.InternalMVCCEntry;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.1
 */
public class SingleKeyNonTxInvocationContext extends AbstractInvocationContext {

   private final boolean isOriginLocal;

   private Object key;

   /**
    * It is possible for the key to only be wrapped but not locked, e.g. when a get takes place.
    */
   private boolean isLocked;

   private CacheEntry cacheEntry;

   private CacheEntry lastReadKey;

   public SingleKeyNonTxInvocationContext(boolean originLocal) {
      isOriginLocal = originLocal;
   }

   @Override
   public boolean isOriginLocal() {
      return isOriginLocal;
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
   public Set<Object> getLockedKeys() {
      return isLocked && key != null ? Collections.singleton(key) : Collections.emptySet();
   }

   @Override
   public void clearLockedKeys() {
      key = null;
      cacheEntry = null;
   }

   @Override
   public void addLockedKey(Object key) {
      if (cacheEntry != null && !key.equals(this.key))
         throw illegalStateException();
      isLocked = true;
   }

   @Override
   public void addRemoteReadKey(Object key, InternalMVCCEntry ime) {
      //no-op
   }

   @Override
   public void addLocalReadKey(Object key, InternalMVCCEntry ime) {
      //no-op
   }

   @Override
   public void removeLocalReadKey(Object key) {
      //no-op
   }

   @Override
   public void removeRemoteReadKey(Object key) {
      //no-op
   }

   @Override
   public InternalMVCCEntry getLocalReadKey(Object Key) {
      return null;  //no-op
   }

   @Override
   public InternalMVCCEntry getRemoteReadKey(Object Key) {
      return null;  //no-op
   }

   @Override
   public void setAlreadyReadOnNode(boolean alreadyRead) {
      //no-op
   }

   @Override
   public boolean getAlreadyReadOnNode() {
      return false; //no-op
   }

   @Override
   public void setLastReadKey(CacheEntry entry) {
      lastReadKey = entry;
   }

   @Override
   public CacheEntry getLastReadKey() {
      return lastReadKey;
   }

   @Override
   public void clearLastReadKey() {
      lastReadKey = null;
   }

   private IllegalStateException illegalStateException() {
      return new IllegalStateException("This is a single key invocation context, using multiple keys shouldn't be possible");
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      if (key != null && key.equals(this.key)) return cacheEntry;
      return null;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return key == null ? Collections.<Object, CacheEntry>emptyMap() : Collections.singletonMap(key, cacheEntry);
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      this.key = key;
      this.cacheEntry = e;
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      if (lookedUpEntries.size() > 1) throw illegalStateException();
      Map.Entry<Object, CacheEntry> e = lookedUpEntries.entrySet().iterator().next();
      this.key = e.getKey();
      this.cacheEntry = e.getValue();
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      if (key.equals(this.key))
         clearLockedKeys();
   }

   @Override
   public void clearLookedUpEntries() {
      clearLockedKeys();
   }

   public CacheEntry getCacheEntry() {
      return cacheEntry;
   }
}
