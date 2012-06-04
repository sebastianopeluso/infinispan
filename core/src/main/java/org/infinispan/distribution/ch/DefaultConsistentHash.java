/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution.ch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The original authors are missing...
 * @author Pedro Ruivo
 * @author Sebastiano Peluso 
 */
public class DefaultConsistentHash extends AbstractWheelConsistentHash {

   private static final Log LOG = LogFactory.getLog(DefaultConsistentHash.class);

   public DefaultConsistentHash() {
   }

   public DefaultConsistentHash(Hash hash) {
      setHashFunction(hash);
   }

   @Override
   public List<Address> locate(final Object key, final int replCount) {
      final int normalizedHash = bypassHashing(key) ? getAssociatedHash(key) : getNormalizedHash(getGrouping(key));
      final int actualReplCount = Math.min(replCount, caches.size());
      final List<Address> owners = new ArrayList<Address>(actualReplCount);
      final boolean virtualNodesEnabled = isVirtualNodesEnabled();

      for (Iterator<Address> it = getPositionsIterator(normalizedHash); it.hasNext();) {
         Address a = it.next();
         // if virtual nodes are enabled we have to avoid duplicate addresses
         boolean isDuplicate = virtualNodesEnabled && owners.contains(a);
         if (!isDuplicate) {
            owners.add(a);
            if (owners.size() >= actualReplCount)
               return owners;
         }
      }

      // might return < replCount owners if there aren't enough nodes in the list
      return owners;
   }

   @Override
   public boolean isKeyLocalToAddress(final Address target, final Object key, final int replCount) {
      final int actualReplCount = Math.min(replCount, caches.size());
      final int normalizedHash = bypassHashing(key) ? getAssociatedHash(key) : getNormalizedHash(getGrouping(key));
      final List<Address> owners = new ArrayList<Address>(actualReplCount);
      final boolean virtualNodesEnabled = isVirtualNodesEnabled();

      for (Iterator<Address> it = getPositionsIterator(normalizedHash); it.hasNext();) {
         Address a = it.next();
         // if virtual nodes are enabled we have to avoid duplicate addresses
         boolean isDuplicate = virtualNodesEnabled && owners.contains(a);
         if (!isDuplicate) {
            if (target.equals(a))
               return true;

            owners.add(a);
            if (owners.size() >= actualReplCount)
               return false;
         }
      }

      return false;
   }

   @Override
   protected Log getLog() {
      return LOG;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DefaultConsistentHash that = (DefaultConsistentHash) o;

      if (hashFunction != null ? !hashFunction.equals(that.hashFunction) : that.hashFunction != null) return false;
      if (numVirtualNodes != that.numVirtualNodes) return false;
      if (caches != null ? !caches.equals(that.caches) : that.caches != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = caches != null ? caches.hashCode() : 0;
      result = 31 * result + hashFunction.hashCode();
      result = 31 * result + numVirtualNodes;
      return result;
   }

   public static class Externalizer extends AbstractWheelConsistentHash.Externalizer<DefaultConsistentHash> {
      @Override
      protected DefaultConsistentHash instance() {
         return new DefaultConsistentHash();
      }

      @Override
      public Integer getId() {
         return Ids.DEFAULT_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends DefaultConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends DefaultConsistentHash>>asSet(DefaultConsistentHash.class);
      }
   }

   public int getDistance(Address a1, Address a2) {
      if (a1 == null || a2 == null) throw new NullPointerException("Cannot deal with nulls as parameters!");

      int p1 = indexOf(a1);
      if (p1 < 0)
         return -1;

      int p2 = indexOf(a2);
      if (p2 < 0)
         return -1;

      if (p1 <= p2)
         return p2 - p1;
      else
         return caches.size() - (p1 - p2);
   }

   public boolean isAdjacent(Address a1, Address a2) {
      int distance = getDistance(a1, a2);
      return distance == 1 || distance == caches.size() - 1;
   }

   public List<Address> getStateProvidersOnJoin(Address self, int replCount) {
      List<Address> l = new LinkedList<Address>();
      List<Address> cachesList = new LinkedList<Address>(caches);
      int selfIdx = cachesList.indexOf(self);
      if (selfIdx >= replCount - 1) {
         l.addAll(cachesList.subList(selfIdx - replCount + 1, selfIdx));
      } else {
         l.addAll(cachesList.subList(0, selfIdx));
         int alreadyCollected = l.size();
         l.addAll(cachesList.subList(cachesList.size() - replCount + 1 + alreadyCollected, cachesList.size()));
      }

      Address plusOne;
      if (selfIdx == cachesList.size() - 1)
         plusOne = cachesList.get(0);
      else
         plusOne = cachesList.get(selfIdx + 1);

      if (!l.contains(plusOne)) l.add(plusOne);
      return l;
   }

   public List<Address> getStateProvidersOnLeave(Address leaver, int replCount) {
      if (trace) getLog().tracef("List of addresses is: %s. leaver is: %s", caches, leaver);
      Set<Address> holders = new HashSet<Address>();
      for (Address address : caches) {
         if (isAdjacent(leaver, address)) {
            holders.add(address);
            if (trace) getLog().tracef("%s is state holder", address);
         } else {
            if (trace) getLog().tracef("%s is NOT state holder", address);
         }
      }
      return new ArrayList<Address>(holders);
   }

   //Sebastiano
   private boolean bypassHashing(Object key){
      return key instanceof StaticGroupSlice;
   }

   //Sebastiano
   private int getAssociatedHash(Object key){
      int slice = ((StaticGroupSlice)key).getSlice();
      int numIndexes = positionKeys.length;
      for (int currentPos = 0; currentPos < numIndexes; ++currentPos) {
         if(currentPos == (slice % numIndexes)){
            return positionKeys[currentPos];
         }
      }
      return positionKeys[0];
   }

   private int indexOf(Address a) {
      int p = 0;
      for (Address target : caches) {
         if (target.equals(a)) return p;
         p++;
      }
      return -1;
   }
}