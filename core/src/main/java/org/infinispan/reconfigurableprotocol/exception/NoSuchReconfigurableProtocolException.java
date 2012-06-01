package org.infinispan.reconfigurableprotocol.exception;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class NoSuchReconfigurableProtocolException extends Exception {
   public NoSuchReconfigurableProtocolException() {
   }

   public NoSuchReconfigurableProtocolException(String s) {
      super(s);
   }

   public NoSuchReconfigurableProtocolException(String s, Throwable throwable) {
      super(s, throwable);
   }

   public NoSuchReconfigurableProtocolException(Throwable throwable) {
      super(throwable);
   }
}
