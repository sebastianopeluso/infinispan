package org.infinispan.reconfigurableprotocol.manager;

import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Manages the current replication protocol in use and synchronize it with the epoch number
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ProtocolManager {

   private static final Log log = LogFactory.getLog(ProtocolManager.class);

   private long epoch = 0;
   private ReconfigurableProtocol current;

   /**
    * init the protocol manager with the initial replication protocol
    *
    * @param actual  the initial replication protocol
    */
   public final synchronized void init(ReconfigurableProtocol actual) {
      this.current = actual;
   }

   /**
    * returns the current replication protocol
    *
    * @return  the current replication protocol
    */
   public final synchronized ReconfigurableProtocol getCurrent() {
      return current;
   }

   /**
    * atomically changes the current protocol and increments the epoch
    *
    * @param newProtocol   the new replication protocol to use
    */
   public final synchronized void changeAndIncrementEpoch(ReconfigurableProtocol newProtocol) {
      current = newProtocol;
      epoch++;
      this.notifyAll();
      if (log.isTraceEnabled()) {
         log.tracef("Changed to new protocol. Current protocol is %s and current epoch is %s",
                    current.getUniqueProtocolName(), epoch);
      }
   }

   /**
    * check if the {@param reconfigurableProtocol} is the current replication protocol in use
    *
    * @param reconfigurableProtocol the replication protocol to check                                   
    * @return                       true if it is the current replication protocol, false otherwise
    */
   public final synchronized boolean isCurrentProtocol(ReconfigurableProtocol reconfigurableProtocol) {
      return reconfigurableProtocol == current || reconfigurableProtocol.equals(current);
   }

   /**
    * atomically returns the current replication protocol and epoch
    *
    * @return  the current replication protocol and epoch
    */
   public final synchronized CurrentProtocolAndEpoch getCurrentProtocolAndEpoch() {
      return new CurrentProtocolAndEpoch(epoch, current);
   }

   /**
    * returns the current epoch
    *
    * @return  the current epoch
    */
   public final synchronized long getEpoch() {
      return epoch;
   }

   /**
    * returns when the current epoch is higher or equals than {@param epoch}, blocking until that condition is true
    *
    * @param epoch                  the epoch to be ensured
    * @throws InterruptedException  if it is interrupted while waiting
    */
   public final synchronized void ensure(long epoch) throws InterruptedException {
      if (log.isDebugEnabled()) {
         log.debugf("[%s] will block until %s >= %s", Thread.currentThread().getName(), epoch, this.epoch);
      }
      while (this.epoch < epoch) {
         this.wait();
      }
      if (log.isDebugEnabled()) {
         log.debugf("[%s] epoch is the desired. Moving on...", Thread.currentThread().getName());
      }
   }

   /**
    * class used to atomically retrieve the current replication protocol and epoch
    */
   public static class CurrentProtocolAndEpoch {
      private final long epoch;
      private final ReconfigurableProtocol current;

      public CurrentProtocolAndEpoch(long epoch, ReconfigurableProtocol current) {
         this.epoch = epoch;
         this.current = current;
      }

      public final long getEpoch() {
         return epoch;
      }

      public final ReconfigurableProtocol getProtocol() {
         return current;
      }

      @Override
      public final String toString() {
         return "CurrentProtocolAndEpoch{" +
               "epoch=" + epoch +
               ", current=" + current +
               '}';
      }
   }
}
