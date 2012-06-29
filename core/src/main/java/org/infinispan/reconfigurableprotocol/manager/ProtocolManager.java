package org.infinispan.reconfigurableprotocol.manager;

import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class ProtocolManager {
   
   private long epoch = 0;
   private ReconfigurableProtocol actual;

   public final synchronized void init(ReconfigurableProtocol actual) {      
      this.actual = actual;
   }   

   public final synchronized ReconfigurableProtocol getActual() {
      return actual;
   }

   public final synchronized void changeAndIncrementEpoch(ReconfigurableProtocol newProtocol) {
      actual = newProtocol;
      epoch++;
      this.notifyAll();
   }

   public final synchronized boolean isActual(ReconfigurableProtocol reconfigurableProtocol) {
      return reconfigurableProtocol == actual || reconfigurableProtocol.equals(actual);
   }
   
   public final synchronized ActualProtocolAndEpoch getActualProtocolAndEpoch() {
      return new ActualProtocolAndEpoch(epoch, actual);
   }

   public final synchronized long getEpoch() {
      return epoch;
   }

   public final synchronized void ensure(long epoch) throws InterruptedException {
      while (this.epoch < epoch) {
         this.wait();
      }
   }
   
   public static class ActualProtocolAndEpoch {
      private final long epoch;
      private final ReconfigurableProtocol actual;

      public ActualProtocolAndEpoch(long epoch, ReconfigurableProtocol actual) {
         this.epoch = epoch;
         this.actual = actual;
      }

      public long getEpoch() {
         return epoch;
      }

      public ReconfigurableProtocol getProtocol() {
         return actual;
      }
   }
}
