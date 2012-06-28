package org.infinispan.reconfigurableprotocol;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.ReconfigurableProtocolCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.*;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

import static org.infinispan.commands.remote.ReconfigurableProtocolCommand.DATA;
import static org.infinispan.interceptors.InterceptorChain.InterceptorType;

/**
 * represents an instance of a replication protocol
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class ReconfigurableProtocol {
   private static final String LOCAL_STOP_ACK = "___LOCAL_ACK___";

   protected Configuration configuration;
   private ComponentRegistry componentRegistry;
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;

   private final Set<GlobalTransaction> localTransactions;
   private final Set<GlobalTransaction> remoteTransactions;

   private final StopAckCollector stopAckCollector;

   public ReconfigurableProtocol() {
      localTransactions = new HashSet<GlobalTransaction>();
      remoteTransactions = new HashSet<GlobalTransaction>();
      stopAckCollector = new StopAckCollector();
   }

   public final void initialize(Configuration configuration, ComponentRegistry componentRegistry) {
      this.configuration = configuration;
      this.componentRegistry = componentRegistry;
      this.rpcManager = getComponent(RpcManager.class);
      this.commandsFactory = getComponent(CommandsFactory.class);
      this.stopAckCollector.reset();
   }

   /**
    * Adds the global transaction to the list of local transactions finished by this protocol
    *
    * @param globalTransaction   the global transaction
    */
   public final void addLocalTransaction(GlobalTransaction globalTransaction) {
      synchronized (localTransactions) {
         localTransactions.add(globalTransaction);
      }
   }

   /**
    * Removes the global transaction to the list of local transactions finished by this protocol
    *
    * @param globalTransaction   the global transaction
    */
   public final void removeLocalTransaction(GlobalTransaction globalTransaction) {
      synchronized (localTransactions) {
         localTransactions.remove(globalTransaction);
         localTransactions.notifyAll();
      }
   }

   /**
    * Adds the global transaction to the list of remote transactions finished by this protocol
    *
    * @param globalTransaction   the global transaction
    */
   public final void addRemoteTransaction(GlobalTransaction globalTransaction) {
      synchronized (remoteTransactions) {
         remoteTransactions.add(globalTransaction);
      }
   }

   /**
    * Removes the global transaction to the list of remote transactions finished by this protocol
    *
    * @param globalTransaction   the global transaction
    */
   public final void removeRemoteTransaction(GlobalTransaction globalTransaction) {
      synchronized (remoteTransactions) {
         remoteTransactions.remove(globalTransaction);
         remoteTransactions.notifyAll();
      }
   }

   /**
    * method invoked when a message is received for this protocol
    *
    * @param data the data in the message
    * @param from the sender    
    */
   public final void handleData(Object data, Address from) {
      if (LOCAL_STOP_ACK.equals(data)) {
         stopAckCollector.addAck(from);
      } else {
         internalHandleData(data,from);
      }
   }

   /**
    * blocks until the local transaction set is empty, i.e., no more local transactions are committing
    *
    * @throws InterruptedException  if interrupted
    */
   protected final void awaitUntilLocalTransactionsFinished() throws InterruptedException {
      synchronized (localTransactions) {
         while (!localTransactions.isEmpty()) {
            localTransactions.wait();
         }
      }
   }

   /**
    * blocks until the remote transaction set is empty, i.e., no more remote transactions are committing
    *
    * @throws InterruptedException  if interrupted
    */
   protected final void awaitUntilRemoteTransactionsFinished() throws InterruptedException {
      synchronized (remoteTransactions) {
         while (!remoteTransactions.isEmpty()) {
            remoteTransactions.wait();
         }
      }
   }

   /**
    * Registers a component in the registry under the given type, and injects any dependencies needed.  If a component
    * of this type already exists, it is overwritten.
    *
    * @param component  component to register
    * @param clazz      type of component
    */
   protected final void registerComponent(Object component, Class<?> clazz) {
      componentRegistry.registerComponent(component, clazz);
   }

   /**
    * it tries to create and register the interceptor.
    * if it exists, it returns the old instance
    * if it does not exits, it register the interceptor in {@param interceptor}
    *
    * @param interceptor      the interceptor to register if it does not exits
    * @param interceptorType  the interceptor type
    * @return                 the instance of the interceptor type
    */
   protected final CommandInterceptor createInterceptor(CommandInterceptor interceptor, Class<? extends CommandInterceptor> interceptorType) {
      CommandInterceptor chainedInterceptor = getComponent(interceptorType);
      if (chainedInterceptor == null) {
         chainedInterceptor = interceptor;
         registerComponent(interceptor, interceptorType);
      }
      return chainedInterceptor;
   }

   /**
    * Retrieves a component of a specified type from the registry, or null if it cannot be found.
    *
    * @param clazz   type to find
    * @return        component, or null
    */
   protected final <T> T getComponent(Class<? extends T> clazz) {
      return componentRegistry.getComponent(clazz);
   }

   protected final void broadcastData(Object data, boolean totalOrder) {
      ReconfigurableProtocolCommand command = commandsFactory.buildReconfigurableProtocolCommand(DATA, getUniqueProtocolName());
      command.setData(data);
      rpcManager.broadcastRpcCommand(command, true, totalOrder);
   }

   protected final EnumMap<InterceptorType, CommandInterceptor> buildDefaultInterceptorChain() {
      EnumMap<InterceptorType, CommandInterceptor> defaultIC = new EnumMap<InterceptorType, CommandInterceptor>(InterceptorType.class);
      //State transfer
      if (configuration.clustering().cacheMode().isDistributed() || configuration.clustering().cacheMode().isReplicated()) {
         defaultIC.put(InterceptorChain.InterceptorType.STATE_TRANSFER,
                       createInterceptor(new StateTransferLockInterceptor(), StateTransferLockInterceptor.class));
      }

      //Locking
      if (configuration.transaction().transactionMode() == TransactionMode.TRANSACTIONAL) {
         if (configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC) {
            defaultIC.put(InterceptorType.LOCKING,
                          createInterceptor(new PessimisticLockingInterceptor(), PessimisticLockingInterceptor.class));
         } else {
            defaultIC.put(InterceptorType.LOCKING,
                          createInterceptor(new OptimisticLockingInterceptor(), OptimisticLockingInterceptor.class));
         }
      } else {
         if (configuration.locking().isolationLevel() != IsolationLevel.NONE)
            defaultIC.put(InterceptorType.LOCKING,
                          createInterceptor(new NonTransactionalLockingInterceptor(), NonTransactionalLockingInterceptor.class));
      }

      //Wrapper
      if (configuration.versioning().enabled() && configuration.clustering().cacheMode().isClustered()) {
         defaultIC.put(InterceptorType.WRAPPER,
                       createInterceptor(new VersionedEntryWrappingInterceptor(), VersionedEntryWrappingInterceptor.class));

      } else {
         defaultIC.put(InterceptorType.WRAPPER,
                       createInterceptor(new EntryWrappingInterceptor(), EntryWrappingInterceptor.class));
      }

      //Deadlock
      if (configuration.deadlockDetection().enabled()) {
         defaultIC.put(InterceptorType.DEADLOCK,
                       createInterceptor(new DeadlockDetectingInterceptor(), DeadlockDetectingInterceptor.class));
      }

      //Clustering interceptor
      switch (configuration.clustering().cacheMode()) {
         case REPL_SYNC:
            if (configuration.versioning().enabled()) {
               defaultIC.put(InterceptorType.CLUSTER,
                             createInterceptor(new VersionedReplicationInterceptor(), VersionedReplicationInterceptor.class));
               break;
            }
         case REPL_ASYNC:
            defaultIC.put(InterceptorType.CLUSTER,
                          createInterceptor(new ReplicationInterceptor(), ReplicationInterceptor.class));
            break;
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            defaultIC.put(InterceptorType.CLUSTER,
                          createInterceptor(new InvalidationInterceptor(), InvalidationInterceptor.class));
            break;
         case DIST_SYNC:
            if (configuration.versioning().enabled()) {
               defaultIC.put(InterceptorType.CLUSTER,
                             createInterceptor(new VersionedDistributionInterceptor(), VersionedDistributionInterceptor.class));
               break;
            }
         case DIST_ASYNC:
            defaultIC.put(InterceptorType.CLUSTER,
                          createInterceptor(new DistributionInterceptor(), DistributionInterceptor.class));
            break;
         case LOCAL:
            //Nothing...
      }
      return defaultIC;
   }

   protected final void globalStopProtocol(boolean totalOrder) throws InterruptedException {
      /*
      1) block local transactions (already done by Manager)
      2) wait until all local transactions has finished
      3) send message signal the end of transactions
      4) wait for others members messages
      5) wait until all remote transactions has finished
      */
      awaitUntilLocalTransactionsFinished();
      broadcastData(LOCAL_STOP_ACK, totalOrder);
      stopAckCollector.awaitAllAck();
      awaitUntilRemoteTransactionsFinished();
      stopAckCollector.reset();
   }

   protected final Collection<Address> getCacheMembers() {
      return rpcManager.getTransport().getMembers();
   }
   
   protected final boolean isCoordinator() {
      return rpcManager.getTransport().isCoordinator();
   }

   /**
    * the global unique protocol name
    *
    * @return  the global unique protocol name
    */
   public abstract String getUniqueProtocolName();

   /**
    * this method switches between te current protocol to the new protocol without ensure this strong condition:
    *  -- no transaction in the current protocol are running in all the system see {@link #stopProtocol()}
    *
    * @param protocol   the new protocol
    * @return           true if the switch is done, false otherwise
    */
   public abstract boolean switchTo(ReconfigurableProtocol protocol);

   /**
    * it ensures that no transactions in the current protocol are running in the system (strong condition). this is
    * necessary to switch to any protocol
    * In other words, it means that all transactions active with that protocol in the cluster need to be ended
    */
   public abstract void stopProtocol() throws InterruptedException;

   /**
    * it starts this new protocol
    */
   public abstract void bootProtocol();

   /**
    * this method check if the {@param globalTransaction} (which contains information about the protocol that is committing it)
    * can be safely committed when this protocol is running.
    *
    * @param globalTransaction   the global transaction    
    * @return                    true if it can be validated, false otherwise
    */
   public abstract boolean canProcessOldTransaction(GlobalTransaction globalTransaction);

   /**
    * one of the first methods to be invoked when this protocol is register. It must register all the components added and
    * possible dependencies
    */
   public abstract void bootstrapProtocol();

   /**
    * creates the interceptor chain for this protocol. if some position in the map is null, the interceptor in that
    * position is bypassed. It is possible to add two custom interceptors: one before Tx Interceptor and another after.
    *
    * Note: the interceptor instances should be instances returned by {@link #createInterceptor(org.infinispan.interceptors.base.CommandInterceptor, Class)}
    *
    * @return  the map with the new interceptors  
    */
   public abstract EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain();

   /**
    * check is this local transaction can be committed via 1 phase, instead of 2 phases. In 1 phase, only the prepare
    * message is created
    *
    * @param localTransaction the local transaction that wants finish
    * @return                 true if it can be committed in 1 phase, false otherwise
    */
   public abstract boolean use1PC(LocalTransaction localTransaction);

   /**
    * method invoked when a message is received for this protocol
    *
    * @param data the data in the message
    * @param from the sender    
    */
   protected abstract void internalHandleData(Object data, Address from);

   private class StopAckCollector {
      //NOTE: it is assuming that nodes will no leave neither join the cache during the switch
      private final Set<Address> members;

      private StopAckCollector() {
         members = new HashSet<Address>();
      }

      public synchronized final void addAck(Address from) {
         members.remove(from);
         if (members.isEmpty()) {
            this.notifyAll();
         }
      }

      public synchronized final void awaitAllAck() throws InterruptedException {
         while (!members.isEmpty()) {
            this.wait();
         }
      }

      public synchronized final void reset() {
         members.clear();
         members.addAll(getCacheMembers());
      }
   }
}
