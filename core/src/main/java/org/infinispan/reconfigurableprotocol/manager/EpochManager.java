package org.infinispan.reconfigurableprotocol.manager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class EpochManager {

   private long epoch = 0;

   public final synchronized long getEpoch() {
      return epoch;
   }

   public final synchronized void incrementEpoch() {
      this.epoch++;
      this.notifyAll();
   }
   
   public final synchronized void ensure(long epoch) throws InterruptedException {
      while (this.epoch < epoch) {
         this.wait();
      }
   }   
}
