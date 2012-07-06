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

   public static enum Type {
      SWITCH(false),
      REGISTER(false),
      DATA(true),
      SWITCH_REQ(false),
      SET_COOL_DOWN_TIME(true);

      final boolean hasData;

      Type(boolean hasData) {
         this.hasData = hasData;
      }
   }

   private ReconfigurableReplicationManager manager;

   private Type type;
   private String protocolId;
   private Object data;

   public ReconfigurableProtocolCommand(String cacheName, Type type, String protocolId) {
      super(cacheName);
      this.type = type;
      this.protocolId = protocolId;
   }

   public ReconfigurableProtocolCommand(String cacheName) {
      super(cacheName);
   }

   public final void init(ReconfigurableReplicationManager manager) {
      this.manager = manager;
   }

   @Override
   public final Object perform(InvocationContext ctx) throws Throwable {
      switch (type) {
         case SWITCH:
            manager.internalSwitchTo(protocolId);
            break;
         case REGISTER:
            manager.internalRegister(protocolId);
            break;
         case DATA:
            manager.handleProtocolData(protocolId, data, getOrigin());
            break;
         case SWITCH_REQ:
            manager.switchTo(protocolId);
            break;
         case SET_COOL_DOWN_TIME:
            manager.setSwitchCoolDownTime((Integer) data);
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
      if (type.hasData) {
         return new Object[] {(byte)type.ordinal(), protocolId, data};
      } else {
         return new Object[] {(byte)type.ordinal(), protocolId};
      }
   }

   @Override
   public final void setParameters(int commandId, Object[] parameters) {
      this.type = Type.values()[(Byte) parameters[0]];
      this.protocolId = (String) parameters[1];
      if (type.hasData) {
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

   @Override
   public String toString() {
      return "ReconfigurableProtocolCommand{" +
            "type=" + type +
            ", protocolId='" + protocolId + '\'' +
            ", data=" + data +
            '}';
   }
}
