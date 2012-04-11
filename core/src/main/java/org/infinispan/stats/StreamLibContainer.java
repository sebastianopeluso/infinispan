package org.infinispan.stats;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This contains all the stream lib top keys. Stream lib is a space efficient technique to obtains the top-most
 * counters.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StreamLibContainer {

   private static final StreamLibContainer instance = new StreamLibContainer();
   public static final int MAX_CAPACITY = 1000;

   private int capacity = 100;
   private boolean active = false;

   private final Map<Stat, StreamSummary<Object>> streamSummaryEnumMap;

   public static enum Stat {
      REMOTE_GET,
      LOCAL_GET,
      REMOTE_PUT,
      LOCAL_PUT,

      MOST_LOCKED_KEYS,
      MOST_CONTENDED_KEYS,
      MOST_FAILED_KEYS,
      MOST_WRITE_SKEW_FAILED_KEYS
   }

   private StreamLibContainer() {
      streamSummaryEnumMap = Collections.synchronizedMap(new EnumMap<Stat, StreamSummary<Object>>(Stat.class));
      clearAll();
   }

   public static StreamLibContainer getInstance() {
      return instance;
   }

   public boolean isActive() {
      return active;
   }

   public void setActive(boolean active) {
      this.active = active;
   }

   public void setCapacity(int capacity) {
      if(capacity <= 0) {
         this.capacity = 1;
      } else {
         this.capacity = capacity;
      }
   }

   public void addGet(Object key, boolean remote) {
      if(!isActive()) {
         return;
      }
      StreamSummary<Object> streamSummary;

      if(remote) {
         streamSummary = streamSummaryEnumMap.get(Stat.REMOTE_GET);
      } else {
         streamSummary = streamSummaryEnumMap.get(Stat.LOCAL_GET);
      }

      offer(streamSummary, key);
   }

   public void addPut(Object key, boolean remote) {
      if(!isActive()) {
         return;
      }

      StreamSummary<Object> streamSummary;

      if(remote) {
         streamSummary = streamSummaryEnumMap.get(Stat.REMOTE_PUT);
      } else {
         streamSummary = streamSummaryEnumMap.get(Stat.LOCAL_PUT);
      }

      offer(streamSummary, key);
   }

   public void addLockInformation(Object key, boolean contention, boolean abort) {
      if(!isActive()) {
         return;
      }

      offer(streamSummaryEnumMap.get(Stat.MOST_LOCKED_KEYS), key);

      if(contention) {
         offer(streamSummaryEnumMap.get(Stat.MOST_CONTENDED_KEYS), key);
      }
      if(abort) {
         offer(streamSummaryEnumMap.get(Stat.MOST_FAILED_KEYS), key);
      }
   }

   public void addWriteSkewFailed(Object key) {
      offer(streamSummaryEnumMap.get(Stat.MOST_WRITE_SKEW_FAILED_KEYS), key);
   }

   public Map<Object, Long> getTopKFrom(Stat stat) {
      return getTopKFrom(stat, capacity);
   }

   public Map<Object, Long> getTopKFrom(Stat stat, int topk) {
      return getStatsFrom(streamSummaryEnumMap.get(stat), topk);

   }

   @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
   private Map<Object, Long> getStatsFrom(StreamSummary<Object> ss, int topk) {
      synchronized (ss) {
         List<Counter<Object>> counters = ss.topK(topk <= 0 ? 1 : topk);
         Map<Object, Long> results = new HashMap<Object, Long>(topk);

         for(Counter<Object> c : counters) {
            results.put(c.getItem(), c.getCount());
         }

         return results;
      }
   }

   public void resetAll(){
      clearAll();
   }

   public void resetStat(Stat stat){
      streamSummaryEnumMap.put(stat, createNewStreamSummary());
   }

   private StreamSummary<Object> createNewStreamSummary() {
      return new StreamSummary<Object>(Math.max(MAX_CAPACITY, capacity));
   }

   private void clearAll() {
      for (Stat stat : Stat.values()) {
         streamSummaryEnumMap.put(stat, createNewStreamSummary());
      }
   }

   @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
   private void offer(StreamSummary<Object> streamSummary, Object key) {
      synchronized (streamSummary) {
         streamSummary.offer(key);
      }
   }
}
