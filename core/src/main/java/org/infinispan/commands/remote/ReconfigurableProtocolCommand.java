package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;

/**
 * Command use when switch between protocol to broadcast data between all members
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ReconfigurableProtocolCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 101;

   public static final byte SWITCH = 1;
   public static final byte REGISTER = 2;
   public static final byte DATA = 3;

   private ReconfigurableReplicationManager reconfigurableReplicationManager;

   private byte type;
   private String protocolId;
   private Object data;

   public ReconfigurableProtocolCommand(String cacheName, byte type, String protocolId) {
      super(cacheName);
      this.type = type;
      this.protocolId = protocolId;
   }

   public ReconfigurableProtocolCommand(String cacheName) {
      super(cacheName);
   }

   public final void init(ReconfigurableReplicationManager reconfigurableReplicationManager) {
      this.reconfigurableReplicationManager = reconfigurableReplicationManager;
   }

   @Override
   public final Object perform(InvocationContext ctx) throws Throwable {
      switch (type) {
         case SWITCH:
            reconfigurableReplicationManager.switchTo(protocolId);
            break;
         case REGISTER:
            reconfigurableReplicationManager.internalRegister(protocolId);
            break;
         case DATA:
            reconfigurableReplicationManager.handleProtocolData(protocolId, data, getOrigin());
            break;
         default:
            break;
      }
      return null;
   }

   @Override
   public final byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public final Object[] getParameters() {
      if (type != DATA) {
         return new Object[] {type, protocolId};
      } else {
         return new Object[] {type, protocolId, data};
      }
   }

   @Override
   public final void setParameters(int commandId, Object[] parameters) {
      this.type = (Byte) parameters[0];
      this.protocolId = (String) parameters[1];
      if (type == DATA) {
         data = parameters[2];
      }
   }

   @Override
   public final boolean isReturnValueExpected() {
      return false;
   }

   public final void setData(Object data) {
      this.data = data;
   }
}
