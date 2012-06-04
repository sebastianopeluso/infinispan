package org.infinispan.mvcc.exception;

import org.infinispan.CacheException;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class ValidationException extends CacheException {

   private final Object key;

   public ValidationException() {
      super();
      this.key = null;
   }

   public ValidationException(Throwable cause, Object key) {
      super(cause);
      this.key = key;
   }

   public ValidationException(String msg, Object key) {
      super(msg);
      this.key = key;
   }

   public ValidationException(String msg, Throwable cause, Object key) {
      super(msg, cause);
      this.key = key;
   }

   public Object getKey() {
      return key;
   }
}
