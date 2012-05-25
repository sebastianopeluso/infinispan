package org.infinispan.reconfigurableprotocol;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.EnumMap;

/**
 * represents an instance of a replication protocol
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class ReconfigurableProtocol {
   protected Configuration configuration;
   private ComponentRegistry componentRegistry;

   protected static enum Interceptor {
      STATE_TRANSFER,
      CUSTOM_INTERCEPTOR_BEFORE_TX_INTERCEPTOR,
      CUSTOM_INTERCEPTOR_AFTER_TX_INTERCEPTOR,
      LOCKING,
      WRAPPER,
      DEADLOCK,
      CLUSTER
   }

   public ReconfigurableProtocol() {}

   public final void initialize(Configuration configuration, ComponentRegistry componentRegistry) {
      this.configuration = configuration;
      this.componentRegistry = componentRegistry;
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

   /**
    * this method switches between te current protocol to the new protocol without ensure this strong condition:
    *  -- no transaction in the current protocol are running in all the system see {@link #stopProtocol()}
    *
    * @param Protocol   the new protocol
    * @return           true if the switch is done, false otherwise
    */
   public abstract boolean switchTo(ReconfigurableProtocol Protocol);

   /**
    * it ensures that no transactions in the current protocol are running in the system (strong condition). this is
    * necessary to switch to any protocol
    * In other words, it means that all transactions active with that protocol in the cluster need to be ended
    */
   public abstract void stopProtocol();

   /**
    * it starts this new protocol
    */
   public abstract void bootProtocol();

   /**
    * this method check if the {@param globalTransaction} (which contains information about the protocol that is committing it)
    * can be safely committed when this protocol is running.
    *
    * @param globalTransaction   the global transaction
    * @param modifications       the modifications done by the transaction
    * @return                    true if it can be validated, false otherwise
    */
   public abstract boolean canProcessTransactionFromPreviousEpoch(GlobalTransaction globalTransaction,
                                                                  WriteCommand[] modifications);

   /**
    * it notifies the finishing of the {@param globalTransaction} validated in the previous epoch
    *
    * @param globalTransaction   the global transaction
    */
   public abstract void finishedProcessOldTransaction(GlobalTransaction globalTransaction);

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
    * @param interceptors  the interceptor map
    */
   public abstract void buildInterceptorChain(EnumMap<Interceptor, CommandInterceptor> interceptors);

   /**
    * check is this local transaction can be committed via 1 phase, instead of 2 phases. In 1 phase, only the prepare
    * message is created
    *
    * @param localTransaction the local transaction that wants finish
    * @return                 true if it can be committed in 1 phase, false otherwise
    */
   public abstract boolean use1PC(LocalTransaction localTransaction);
}
