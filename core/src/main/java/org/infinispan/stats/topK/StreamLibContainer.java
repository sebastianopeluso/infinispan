/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
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
package org.infinispan.stats.topK;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This contains all the stream lib top keys. Stream lib is a space efficient technique to obtains the top-most
 * counters.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StreamLibContainer {

   public static final int MAX_CAPACITY = 100000;
   private static final Log log = LogFactory.getLog(StreamLibContainer.class);
   private final String cacheName;
   private final String address;
   private volatile int capacity = 1000;
   private volatile boolean active = false;
   private volatile FlushThread flushThread;

   public StreamLibContainer(String cacheName, String address) {
      this.cacheName = cacheName;
      this.address = address;
      resetAll();
   }

   public static StreamLibContainer getOrCreateStreamLibContainer(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      StreamLibContainer streamLibContainer = componentRegistry.getComponent(StreamLibContainer.class);
      if (streamLibContainer == null) {
         String cacheName = cache.getName();
         String address = String.valueOf(cache.getCacheManager().getAddress());
         componentRegistry.registerComponent(new StreamLibContainer(cacheName, address), StreamLibContainer.class);
      }
      return componentRegistry.getComponent(StreamLibContainer.class);
   }

   public boolean isActive() {
      return active;
   }

   public void setActive(boolean active) {
      if (!this.active && active) {
         this.flushThread = new FlushThread(cacheName);
         this.flushThread.start();
         resetAll(capacity);
      } else if (!active) {
         if (flushThread != null) {
            flushThread.terminate();
            flushThread = null;
         }
         resetAll(1);
      }
      this.active = active;
   }

   public int getCapacity() {
      return capacity;
   }

   public void setCapacity(int capacity) {
      if (capacity <= 0) {
         this.capacity = 1;
      } else {
         this.capacity = capacity;
      }
   }

   public void addGet(Object key, boolean remote) {
      if (!isActive()) {
         return;
      }
      syncOffer(remote ? Stat.REMOTE_GET : Stat.LOCAL_GET, key);
   }

   public void addPut(Object key, boolean remote) {
      if (!isActive()) {
         return;
      }

      syncOffer(remote ? Stat.REMOTE_PUT : Stat.LOCAL_PUT, key);
   }

   public void addLockInformation(Object key, boolean contention, boolean abort) {
      if (!isActive()) {
         return;
      }

      syncOffer(Stat.MOST_LOCKED_KEYS, key);

      if (contention) {
         syncOffer(Stat.MOST_CONTENDED_KEYS, key);
      }
      if (abort) {
         syncOffer(Stat.MOST_FAILED_KEYS, key);
      }
   }

   public void addWriteSkewFailed(Object key) {
      syncOffer(Stat.MOST_WRITE_SKEW_FAILED_KEYS, key);
   }

   public Map<Object, Long> getTopKFrom(Stat stat) {
      return getTopKFrom(stat, capacity);
   }

   public Map<Object, Long> getTopKFrom(Stat stat, int topK) {
      stat.flush();
      return stat.streamSummary.topKey(topK);
   }

   public Map<String, Long> getTopKFromAsKeyString(Stat stat) {
      return getTopKFromAsKeyString(stat, capacity);
   }

   public Map<String, Long> getTopKFromAsKeyString(Stat stat, int topK) {
      stat.flush();
      return stat.streamSummary.topKeyAsKeyString(topK);
   }

   public void resetAll() {
      resetAll(capacity);
   }

   public void resetStat(Stat stat) {
      resetStat(stat, capacity);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StreamLibContainer that = (StreamLibContainer) o;

      return !(address != null ? !address.equals(that.address) : that.address != null) && !(cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null);

   }

   @Override
   public int hashCode() {
      int result = cacheName != null ? cacheName.hashCode() : 0;
      result = 31 * result + (address != null ? address.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "StreamLibContainer{" +
            "cacheName='" + cacheName + '\'' +
            ", address='" + address + '\'' +
            '}';
   }

   protected ConcurrentStreamSummary<Object> createNewStreamSummary() {
      return createNewStreamSummary(capacity);
   }

   private void resetStat(Stat stat, int customCapacity) {
      stat.streamSummary = createNewStreamSummary(customCapacity);
   }

   private ConcurrentStreamSummary<Object> createNewStreamSummary(int customCapacity) {
      return new ConcurrentStreamSummary<Object>(Math.min(MAX_CAPACITY, customCapacity));
   }

   private void resetAll(int customCapacity) {
      for (Stat stat : Stat.values()) {
         resetStat(stat, customCapacity);
      }
   }

   private void syncOffer(final Stat stat, Object key) {
      if (log.isTraceEnabled()) {
         log.tracef("Offer key=%s to stat=%s in %s", key, stat, this);
      }
      stat.offer(key, flushThread);
   }

   public static enum Stat {
      REMOTE_GET,
      LOCAL_GET,
      REMOTE_PUT,
      LOCAL_PUT,

      MOST_LOCKED_KEYS,
      MOST_CONTENDED_KEYS,
      MOST_FAILED_KEYS,
      MOST_WRITE_SKEW_FAILED_KEYS;
      private final BlockingQueue<Object> pendingOffers = new LinkedBlockingQueue<Object>();
      private volatile ConcurrentStreamSummary<Object> streamSummary;

      public final void offer(final Object element, final FlushThread flushThread) {
         pendingOffers.add(element);
         if (pendingOffers.size() > 1000000 && flushThread != null) {
            flushThread.flush();
         }
      }

      public final void reset(StreamLibContainer container) {
         pendingOffers.clear();
         streamSummary = container.createNewStreamSummary();
      }

      public final void flush() {
         ConcurrentStreamSummary<Object> summary = streamSummary;
         List<Object> keys = new ArrayList<Object>();
         pendingOffers.drainTo(keys);
         Map<Object, Counter> counterMap = new HashMap<Object, Counter>();
         for (Object k : keys) {
            Counter counter = counterMap.get(k);
            if (counter == null) {
               counter = new Counter();
               counterMap.put(k, counter);
            }
            counter.value++;
         }
         for (Map.Entry<Object, Counter> entry : counterMap.entrySet()) {
            summary.offer(entry.getKey(), entry.getValue().value);
         }
      }
   }

   private static class Counter {
      private int value = 0;
   }

   private class FlushThread extends Thread {

      private volatile boolean running;

      public FlushThread(String cacheName) {
         super(cacheName + "-topk-flusher");
      }

      @Override
      public void run() {
         running = true;
         while (running) {
            try {
               sleep();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               continue;
            }
            for (Stat stat : Stat.values()) {
               stat.flush();
            }
         }
      }

      @Override
      public final void interrupt() {
         running = false;
         super.interrupt();
      }

      public synchronized final void terminate() {
         notify();
         running = false;
      }

      public synchronized final void flush() {
         notify();
      }

      private synchronized void sleep() throws InterruptedException {
         wait(60000);
      }
   }
}
