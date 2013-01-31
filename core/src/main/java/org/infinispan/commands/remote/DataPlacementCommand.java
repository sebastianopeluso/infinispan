package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.dataplacement.ObjectRequest;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The command used to send information among nodes
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementCommand extends BaseRpcCommand {

   private static final Log log = LogFactory.getLog(DataPlacementCommand.class);

   public static final short COMMAND_ID = 102;

   public static enum Type {
      /**
       * send to the coordinator to request the start of the algorithm
       */
      DATA_PLACEMENT_REQUEST,

      /**
       * coordinator broadcast this to start the algorithm
       */
      DATA_PLACEMENT_START,

      /**
       * contains the remote top list
       */
      REMOTE_TOP_LIST_PHASE,

      /**
       * contains the object lookup
       */
      OBJECT_LOOKUP_PHASE,

      /**
       * contains the final ACK to start the state transfer
       */
      ACK_COORDINATOR_PHASE,

      /**
       * sets the new cool down period
       */
      SET_COOL_DOWN_TIME
   }

   private DataPlacementManager dataPlacementManager;

   //message data
   private Type type;
   private long roundId;
   private int coolDownTime;
   private ObjectRequest objectRequest;
   private ObjectLookup objectLookup;
   private Address[] members;


   public DataPlacementCommand(String cacheName, Type type, long roundId) {
      super(cacheName);
      this.type = type;
      this.roundId = roundId;
   }

   public DataPlacementCommand(String cacheName) {
      super(cacheName);
   }

   public final void initialize(DataPlacementManager dataPlacementManager) {
      this.dataPlacementManager = dataPlacementManager;
   }

   public void setObjectRequest(ObjectRequest objectRequest) {
      this.objectRequest = objectRequest;
   }

   public void setObjectLookup(ObjectLookup objectLookup) {
      this.objectLookup = objectLookup;
   }

   public void setCoolDownTime(int coolDownTime) {
      this.coolDownTime = coolDownTime;
   }

   public void setMembers(Address[] members) {
      this.members = members;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      try {
         switch (type) {
            case DATA_PLACEMENT_REQUEST:
               dataPlacementManager.dataPlacementRequest();
               break;
            case DATA_PLACEMENT_START:
               dataPlacementManager.startDataPlacement(roundId, members);
               break;
            case REMOTE_TOP_LIST_PHASE:
               dataPlacementManager.addRequest(getOrigin(), objectRequest, roundId);
               break;
            case OBJECT_LOOKUP_PHASE:
               dataPlacementManager.addObjectLookup(getOrigin(), objectLookup, roundId);
               break;
            case ACK_COORDINATOR_PHASE:
               dataPlacementManager.addAck(roundId, getOrigin());
               break;
            case SET_COOL_DOWN_TIME:
               dataPlacementManager.internalSetCoolDownTime(coolDownTime);
               break;
         }
      } catch (Exception e) {
         log.errorf(e, "Exception caught while processing command. Type is %s", type);
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      switch (type) {
         case DATA_PLACEMENT_REQUEST:
            return new Object[] {(byte) type.ordinal()};
         case DATA_PLACEMENT_START:
            Object[] retVal = new Object[2 + (members == null ? 0 : members.length)];
            retVal[0] = (byte) type.ordinal();
            retVal[1] = roundId;
            if (members != null && members.length != 0) {
               System.arraycopy(members, 0, retVal, 2, members.length);
            }
            return retVal;
         case ACK_COORDINATOR_PHASE:
            return new Object[] {(byte) type.ordinal(), roundId};
         case REMOTE_TOP_LIST_PHASE:
            return new Object[] {(byte) type.ordinal(), roundId, objectRequest};
         case OBJECT_LOOKUP_PHASE:
            return new Object[] {(byte) type.ordinal(), roundId, objectLookup};
         case SET_COOL_DOWN_TIME:
            return new Object[] {(byte) type.ordinal(), coolDownTime};
      }
      throw new IllegalStateException("This should never happen!");
   }

   @SuppressWarnings("unchecked")
   @Override
   public void setParameters(int commandId, Object[] parameters) {
      type = Type.values()[(Byte)parameters[0]];

      switch (type) {
         case DATA_PLACEMENT_START:
            roundId = (Long) parameters[1];
            if (parameters.length > 2) {
               members = new Address[parameters.length - 2];
               for (int i = 0; i < members.length; ++i) {
                  members[i] = (Address) parameters[i + 2];
               }
            }
            break;
         case ACK_COORDINATOR_PHASE:
            roundId = (Long) parameters[1];
            break;
         case REMOTE_TOP_LIST_PHASE:
            roundId = (Long) parameters[1];
            objectRequest = (ObjectRequest) parameters[2];
            break;
         case OBJECT_LOOKUP_PHASE:
            roundId = (Long) parameters[1];
            objectLookup = (ObjectLookup) parameters[2];
            break;
         case SET_COOL_DOWN_TIME:
            coolDownTime = (Integer) parameters[1];
            break;
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }
}
