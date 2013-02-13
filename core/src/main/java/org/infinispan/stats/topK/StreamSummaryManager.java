package org.infinispan.stats.topK;

import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.concurrent.ConcurrentMapFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StreamSummaryManager {
   
   private static final ConcurrentMap<String, StreamLibContainer> STREAM_LIB_CONTAINER = ConcurrentMapFactory.makeConcurrentMap();
   
   public static StreamLibContainer getStreamLibForCache(String cacheName) {
      StreamLibContainer container = STREAM_LIB_CONTAINER.get(cacheName);
      if (container == null) {
         StreamLibContainer old = STREAM_LIB_CONTAINER.putIfAbsent(cacheName, new StreamLibContainer());
         return old != null ? old : container;
      }
      return container;
   }
   
}
