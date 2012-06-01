package org.infinispan.reconfigurableprotocol.manager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class EpochManager {

   private long epoch = 0;

   public final synchronized long getEpoch() {
      return epoch;
   }

   public final synchronized void incrementEpoch() {
      this.epoch++;
   }
}
