package org.infinispan.reconfigurableprotocol;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.ReconfigurableProtocolCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.reconfigurableprotocol.exception.AlreadyRegisterProtocolException;
import org.infinispan.reconfigurableprotocol.exception.NoSuchReconfigurableProtocolException;
import org.infinispan.reconfigurableprotocol.manager.EpochManager;
import org.infinispan.reconfigurableprotocol.manager.ProtocolManager;
import org.infinispan.reconfigurableprotocol.protocol.PassiveReplicationCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TotalOrderCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TwoPhaseCommitProtocol;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.ReclosableLatch;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.infinispan.commands.remote.ReconfigurableProtocolCommand.REGISTER;
import static org.infinispan.commands.remote.ReconfigurableProtocolCommand.SWITCH;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
//TODO add jmx interface to switch and register new protocol
@MBean(objectName = "ReconfigurableReplicationManager", description = "Manages the replication protocol used to commit" +
      " the transactions and for the switching between them")
public class ReconfigurableReplicationManager {

   private final ReconfigurableProtocolRegistry registry;
   private final EpochManager epochManager = new EpochManager();
   private final ProtocolManager protocolManager = new ProtocolManager();
   private final ReclosableLatch switchInProgress = new ReclosableLatch(true);

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;

   public ReconfigurableReplicationManager() {
      registry = new ReconfigurableProtocolRegistry();
   }

   @Inject
   public void inject(InterceptorChain interceptorChain, RpcManager rpcManager, CommandsFactory commandsFactory,
                      Configuration configuration) {
      registry.inject(interceptorChain);
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;

      ReconfigurableProtocol protocol = new TwoPhaseCommitProtocol();
      try {
         registry.registerNewProtocol(protocol);
      } catch (AlreadyRegisterProtocolException e) {
         //ignore
      }
      protocol = new PassiveReplicationCommitProtocol();
      try {
         registry.registerNewProtocol(protocol);
      } catch (AlreadyRegisterProtocolException e) {
         //ignore
      }
      protocol = new TotalOrderCommitProtocol();
      try {
         registry.registerNewProtocol(protocol);
      } catch (AlreadyRegisterProtocolException e) {
         //ignore
      }
      
      switch (configuration.transaction().transactionProtocol()) {
         case TOTAL_ORDER:
            protocol = registry.getProtocolById(TotalOrderCommitProtocol.UID);
            break;
         case TWO_PHASE_COMMIT:
            protocol = registry.getProtocolById(TwoPhaseCommitProtocol.UID);
            break;
         case PASSIVE_REPLICATION:
            protocol = registry.getProtocolById(PassiveReplicationCommitProtocol.UID);
            break;
      }
      protocolManager.init(protocol);
   }

   /**
    * switch the replication protocol with the new. the switch will not happen if you try to switch to the same 
    * replication protocol or the new protocol does not exist
    *
    * Note:
    *  1) first it tries to use the non-blocking switch (switchTo method in protocol)
    *  2) if the first fails, it uses the stop-the-world model
    *
    * @param protocolId                               the new protocol ID
    * @throws NoSuchReconfigurableProtocolException   if the new protocol does not exist
    * @throws InterruptedException                    if it is interrupted
    */
   public final void internalSwitchTo(String protocolId) throws NoSuchReconfigurableProtocolException, InterruptedException {
      ReconfigurableProtocol newProtocol = registry.getProtocolById(protocolId);
      if (newProtocol == null) {
         throw new NoSuchReconfigurableProtocolException("Protocol ID " + protocolId + " not found");
      } else if (protocolManager.isActual(newProtocol)) {
         return; //nothing to do
      }

      switchInProgress.close();
      ReconfigurableProtocol actual = protocolManager.getActual();
      if (!actual.switchTo(newProtocol)) {
         actual.stopProtocol();
         newProtocol.bootProtocol();
      }
      protocolManager.change(newProtocol);
      epochManager.incrementEpoch();
      switchInProgress.open();
   }

   /**
    * method invoked when a message is received from the network. it contains data for the specific replication protocol
    *
    * @param protocolId the target protocol Id
    * @param data       the data
    * @param from       the sender
    */
   public final void handleProtocolData(String protocolId, Object data, Address from) {
      ReconfigurableProtocol protocol = registry.getProtocolById(protocolId);
      protocol.handleData(data, from);
   }

   /**
    * notifies the protocol to a new local transaction that wants to commit.
    * sets the epoch and the protocol to use for this transaction.     
    * it blocks if a switch between protocols is in progress.
    *
    * @param globalTransaction      the global transaction
    * @throws InterruptedException  if interrupted while waiting for the switch to finish
    */
   public final void notifyLocalTransaction(GlobalTransaction globalTransaction) throws InterruptedException {
      //returns immediately if no switch is in progress
      switchInProgress.await();
      long epoch = epochManager.getEpoch();
      ReconfigurableProtocol actual = protocolManager.getActual();

      globalTransaction.setEpochId(epoch);
      globalTransaction.setProtocolId(actual.getUniqueProtocolName());
      globalTransaction.setReconfigurableProtocol(actual);

      actual.addLocalTransaction(globalTransaction);
   }

   /**
    * notifies the actual protocol for a remote transaction. if the transaction epoch is lower than the actual epoch
    * then the actual protocol is notified and decides if the transaction can commit or should be aborted
    *
    * if a transaction with higher epoch is received then it blocks it until the epoch changes
    *
    * @param globalTransaction      the global transaction
    * @throws InterruptedException  if interrupted while waiting for the new epoch
    */
   public final void notifyRemoteTransaction(GlobalTransaction globalTransaction) throws InterruptedException {
      long txEpoch = globalTransaction.getEpochId();
      long epoch = epochManager.getEpoch();
      ReconfigurableProtocol protocol = registry.getProtocolById(globalTransaction.getProtocolId());
      globalTransaction.setReconfigurableProtocol(protocol);

      if (txEpoch < epoch) {
         ReconfigurableProtocol actual = protocolManager.getActual();
         if (!actual.canProcessOldTransaction(globalTransaction)) {
            throw new CacheException("Cannot commit transaction from previous epoch");
         }
      }
      epochManager.ensure(txEpoch);
      protocol.addRemoteTransaction(globalTransaction);
   }

   /**
    * notifies the ending of the local transaction
    *
    * @param globalTransaction   the global transaction
    */
   public final void notifyLocalTransactionFinished(GlobalTransaction globalTransaction) {
      ReconfigurableProtocol protocol = registry.getProtocolById(globalTransaction.getProtocolId());
      protocol.removeLocalTransaction(globalTransaction);
   }

   /**
    * notifies the ending of the remote transaction
    *
    * @param globalTransaction   the global transaction
    */
   public final void notifyRemoteTransactionFinished(GlobalTransaction globalTransaction) {
      ReconfigurableProtocol protocol = registry.getProtocolById(globalTransaction.getProtocolId());
      protocol.removeRemoteTransaction(globalTransaction);
   }

   /**
    * register a new replication protocol in the ReconfigurableProtocolRegistry. 
    *
    * @param clazzName  the full class name
    * @throws Exception if it was not registered, due to the class does not extend ReconfigurableProtocol or
    *                   the protocol is already registered
    */
   public final void internalRegister(String clazzName) throws Exception {
      Class<?> clazz = Util.loadClass(clazzName, this.getClass().getClassLoader());
      if (!ReconfigurableProtocol.class.isAssignableFrom(clazz)) {
         throw new Exception("Class " + clazzName + " does not extends ReconfigurableProtocol class");
      }
      ReconfigurableProtocol newProtocol = (ReconfigurableProtocol) clazz.newInstance();
      registry.registerNewProtocol(newProtocol);
   }

   /**
    * Returns the information about the protocol, namely the protocol ID and the full class name
    *
    * @param protocol   the protocol
    * @return           the information about the protocol, namely the protocol ID and the full class name
    */
   private String[] getProtocolInfo(ReconfigurableProtocol protocol) {
      String[] info = new String[2];
      info[0] = protocol.getUniqueProtocolName();
      info[1] = protocol.getClass().getCanonicalName();
      return info;
   }

   @ManagedOperation(description = "Registers a new replication protocol. The new protocol must extend the " +
         "ReconfigurableProtocol")
   public void register(String clazzName) throws Exception {
      try {
         internalRegister(clazzName);
         ReconfigurableProtocolCommand command = commandsFactory.buildReconfigurableProtocolCommand(REGISTER, clazzName);
         rpcManager.broadcastRpcCommand(command, false, false);
      } catch (Exception e) {
         throw new Exception("Exception while registering class: " + e.getMessage());
      }
   }

   @ManagedOperation(description = "Switch the current replication protocol for the new one. It fails if the protocol " +
         "does not exists or it is equals to the current")
   public final void switchTo(String protocolId) throws Exception {
      //TODO add a time before perform the switch 
      try {
         ReconfigurableProtocolCommand command = commandsFactory.buildReconfigurableProtocolCommand(SWITCH, protocolId);
         rpcManager.broadcastRpcCommand(command, false, false);
         internalSwitchTo(protocolId);
      } catch (Exception e) {
         throw new Exception("Exception while switching protocols: " + e.getMessage());
      }
   }

   @ManagedAttribute(description = "Returns a collection of replication protocols IDs that can be used in the switchTo",
                     writable = false)
   public final Collection<String> getAvailableProtocolIds() {
      Collection<ReconfigurableProtocol> protocols = registry.getAllAvailableProtocols();
      List<String> result = new LinkedList<String>();
      for (ReconfigurableProtocol p : protocols) {
         result.add(p.getUniqueProtocolName());
      }
      return result;
   }

   @ManagedAttribute(description = "Returns a collection with the information about the replication protocols available, " +
         "namely, the protocol ID and the class name", writable = false)
   public final Collection<String[]> getAvailableProtocols() {
      Collection<ReconfigurableProtocol> protocols = registry.getAllAvailableProtocols();
      List<String[]> result = new LinkedList<String[]>();
      for (ReconfigurableProtocol p : protocols) {
         result.add(getProtocolInfo(p));
      }
      return result;
   }

   @ManagedAttribute(description = "Returns the current replication protocol ID", writable = false)
   public final String getActualProtocolId() {
      return protocolManager.getActual().getUniqueProtocolName();
   }

   @ManagedAttribute(description = "Returns the current replication protocol information, namely the protocol ID and " +
         "the class name", writable = false)
   public final String[] getActualProtocol() {
      return getProtocolInfo(protocolManager.getActual());
   }
}