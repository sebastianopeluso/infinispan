package org.infinispan.reconfigurableprotocol.manager;

import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class ProtocolManager {

   private ReconfigurableProtocol previous;
   private ReconfigurableProtocol actual;

   public final synchronized void init(ReconfigurableProtocol actual) {
      this.previous = null;
      this.actual = actual;
   }

   public final synchronized ReconfigurableProtocol getPrevious() {
      return previous;
   }

   public final synchronized ReconfigurableProtocol getActual() {
      return actual;
   }

   public final synchronized void change(ReconfigurableProtocol newProtocol) {
      previous = actual;
      actual = newProtocol;
   }

   public final synchronized boolean isActual(ReconfigurableProtocol reconfigurableProtocol) {
      return reconfigurableProtocol == actual || reconfigurableProtocol.equals(actual);
   }
}
