package org.infinispan.reconfigurableprotocol;

import org.infinispan.reconfigurableprotocol.exception.AlreadyRegisterProtocolException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class responsible to keep all the possible ReconfigurableProtocol for this cache. It manages internally the Id of
 * each protocol
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ReconfigurableProtocolRegistry {
   public static final short UNKNOWN_PROTOCOL = -1;

   private final Map<Short, ReconfigurableProtocol> idsToProtocol;
   private short nextProtocolId;

   public ReconfigurableProtocolRegistry() {
      this.idsToProtocol = new ConcurrentHashMap<Short, ReconfigurableProtocol>();
      this.nextProtocolId = 0;
   }

   /**
    * returns the current ids and replication protocols currently register
    *
    * @return  the current ids and replication protocols currently register
    */
   public final Map<Short, ReconfigurableProtocol> getAvailableReconfigurableProtocols() {
      return Collections.unmodifiableMap(idsToProtocol);
   }

   /**
    * registers a new protocol to this registry and set it an id
    *
    * @param protocol                           the new protocol
    * @throws AlreadyRegisterProtocolException  if the protocol is already register
    */
   public final synchronized void registerNewReconfigurableProtocol(ReconfigurableProtocol protocol)
         throws AlreadyRegisterProtocolException {
      if (idsToProtocol.values().contains(protocol)) {
         throw new AlreadyRegisterProtocolException(protocol);
      }
      idsToProtocol.put(nextProtocolId++, protocol);
      //TODO bootstrap protocol
      //TODO broadcast to everybody (??)
   }

   /**
    * returns the protocol associated to this protocol id
    *
    * @param protocolId the protocol id
    * @return           the reconfigurable protocol instance or null if the protocol id does not exists
    */
   public final ReconfigurableProtocol getReconfigurableProtocol(short protocolId) {
      return idsToProtocol.get(protocolId);
   }

   /**
    * returns the id of the protocol
    *
    * @param protocol   the reconfigurable protocol
    * @return           the protocol id or UNKNOWN_PROTOCOL if the protocol is not registered
    */
   public final short getReconfigurableProtocolById(ReconfigurableProtocol protocol) {
      if (protocol == null) {
         return UNKNOWN_PROTOCOL;
      }
      for (Map.Entry<Short, ReconfigurableProtocol> entry : idsToProtocol.entrySet()) {
         if (entry.getValue().equals(protocol)) {
            return entry.getKey();
         }
      }
      return UNKNOWN_PROTOCOL;
   }
}
