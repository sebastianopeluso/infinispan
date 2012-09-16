package org.infinispan.dataplacement;

import java.util.Collections;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectRequest {

   private final Map<Object, Long> remoteAccesses;
   private final Map<Object, Long> localAccesses;

   public ObjectRequest(Map<Object, Long> remoteAccesses, Map<Object, Long> localAccesses) {
      this.remoteAccesses = remoteAccesses;
      this.localAccesses = localAccesses;
   }

   public Map<Object, Long> getRemoteAccesses() {
      return remoteAccesses == null ? Collections.<Object, Long>emptyMap() : remoteAccesses;
   }

   public Map<Object, Long> getLocalAccesses() {
      return localAccesses == null ? Collections.<Object, Long>emptyMap() : localAccesses;
   }

   @Override
   public String toString() {
      return "ObjectRequest{" +
            "remoteAccesses=" + (remoteAccesses == null ? 0 : remoteAccesses.size()) +
            ", localAccesses=" + (localAccesses == null ? 0 : localAccesses.size()) +
            '}';
   }

   public String toString(boolean detailed) {
      if (detailed) {
         return "ObjectRequest{" +
               "remoteAccesses=" + remoteAccesses +
               ", localAccesses=" + localAccesses +
               '}';
      }
      return toString();
   }
   
   public int getSerializedSize() {
      return 2;
   }

   public static Object[] getParameters(ObjectRequest request) {
      return new Object[] {request.localAccesses, request.remoteAccesses};
   }

   @SuppressWarnings("unchecked")
   public static ObjectRequest setParameters(Object[] parameters) {
      if (parameters.length != 2) {
         return null;
      }
      return new ObjectRequest((Map<Object, Long>) parameters[0], (Map<Object, Long>) parameters[1]);
   }
}
