package org.infinispan.stats.topK;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ReplicatedStreamSummaryInterceptor extends StreamSummaryInterceptor {
   @Override
   protected boolean isRemote(Object key) {
      return false;
   }
}
