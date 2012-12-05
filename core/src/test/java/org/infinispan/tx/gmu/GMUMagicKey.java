package org.infinispan.tx.gmu;

import org.infinispan.Cache;

import java.io.Serializable;
import java.util.Random;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.distribution.DistributionTestHelper.isOwner;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUMagicKey implements Serializable {
   private final String name;
   private final int hashCode;
   private final String mapTo;
   private final String notMapTo;

   public GMUMagicKey(Cache<?, ?> toMapTo, Cache<?,?> notToMapTo, String name) {
      if (toMapTo == null && notToMapTo == null) {
         throw new NullPointerException("Both map to and not map to cannot be null");
      }

      mapTo = toMapTo == null ? "N/A" : addressOf(toMapTo).toString();
      notMapTo = notToMapTo == null ? "N/A" : addressOf(notToMapTo).toString();

      Random r = new Random();
      Object dummy;
      do {
         // create a dummy object with this hashCode
         final int hc = r.nextInt();
         dummy = new Object() {
            @Override
            public int hashCode() {
               return hc;
            }
         };

      } while (!checkCondition(toMapTo, notToMapTo, dummy));

      // we have found a hashCode that works!
      hashCode = dummy.hashCode();
      this.name = name;
   }

   private boolean checkCondition(Cache<?,?> toMapTo, Cache<?,?> notToMapTo, Object dummy) {
      if (toMapTo == null) {
         return !isOwner(notToMapTo, dummy);
      } else if (notToMapTo == null) {
         return isOwner(toMapTo, dummy);
      }
      return isOwner(toMapTo, dummy) && !isOwner(notToMapTo, dummy);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GMUMagicKey that = (GMUMagicKey) o;

      return hashCode == that.hashCode &&
            !(mapTo != null ? !mapTo.equals(that.mapTo) : that.mapTo != null) &&
            !(notMapTo != null ? !notMapTo.equals(that.notMapTo) : that.notMapTo != null);

   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public String toString() {
      return "GMUMagicKey{" +
            (name == null ? "" : "name='" + name + '\'') +
            ", hashCode=" + hashCode +
            ", mapTo='" + mapTo + '\'' +
            ", notMapTo='" + notMapTo + '\'' +
            '}';
   }
}
