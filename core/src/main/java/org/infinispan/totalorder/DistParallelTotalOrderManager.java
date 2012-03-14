package org.infinispan.totalorder;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.PrepareResponseCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.totalorder.TotalOrderRemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistParallelTotalOrderManager extends ParallelTotalOrderManager {

   private CommandsFactory commandsFactory;
   private DistributionManager distributionManager;
   private ConcurrentMap<GlobalTransaction, VersionsCollector> versionsCollectorMap =
         new ConcurrentHashMap<GlobalTransaction, VersionsCollector>();

   @Inject
   public void inject(CommandsFactory commandsFactory, DistributionManager distributionManager) {
      this.commandsFactory = commandsFactory;
      this.distributionManager = distributionManager;
   }

   @Override
   protected void afterAddLocalTransaction(GlobalTransaction globalTransaction, LocalTransaction localTransaction) {
      Set<Object> keys = getModifiedKeys(localTransaction.getModifications());
      versionsCollectorMap.put(globalTransaction, new VersionsCollector(keys));
   }

   @Override
   protected void afterFinishTransaction(GlobalTransaction globalTransaction) {
      versionsCollectorMap.remove(globalTransaction);
   }

   @Override
   public void addVersions(GlobalTransaction gtx, Throwable exception, Set<Object> keysValidated) {
      VersionsCollector versionsCollector = versionsCollectorMap.get(gtx);
      if (versionsCollector != null) {
         if (exception != null) {
            if (versionsCollector.addException(exception)) {
               updateLocalTransaction(versionsCollector.getException(), true, gtx);
            }
         } else if (versionsCollector.addKeysValidated(keysValidated)) {
            updateLocalTransaction(null, false, gtx);
         }
      } else {
         //TODO log in debug or trace
      }
   }

   @Override
   protected MultiThreadValidation buildMultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext ctx,
                                                              CommandInterceptor invoker,
                                                              TotalOrderRemoteTransaction totalOrderRemoteTransaction) {
      return new DistMultiThreadValidation(prepareCommand, ctx, invoker, totalOrderRemoteTransaction);
   }

   private class DistMultiThreadValidation extends MultiThreadValidation {

      private DistMultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                        CommandInterceptor invoker, TotalOrderRemoteTransaction totalOrderRemoteTransaction) {
         super(prepareCommand, txInvocationContext, invoker, totalOrderRemoteTransaction);
      }

      @Override
      protected void finalizeValidation(Object result, boolean exception) {
         if (prepareCommand.isOnePhaseCommit()) {
            super.finalizeValidation(result, exception);
            return;
         }
         remoteTransaction.markPreparedAndNotify();
         //result is the new versions (or an exception)
         GlobalTransaction gtx = prepareCommand.getGlobalTransaction();
         VersionsCollector versionsCollector = versionsCollectorMap.get(gtx);
         if (versionsCollector != null) {
            if (exception) {
               if (versionsCollector.addException((Throwable) result)) {
                  updateLocalTransaction(versionsCollector.getException(), true, gtx);
               }
            } else if (versionsCollector.addKeysValidated(getLocalKeys(prepareCommand.getModifications()))) {
               updateLocalTransaction(null, false, gtx);
            }
         } else {
            //send the response            
            PrepareResponseCommand prepareResponseCommand = commandsFactory.buildPrepareResponseCommand(gtx);
            if (exception) {
               prepareResponseCommand.setException((Throwable) result);
            } else {
               Set<Object> keysValidated = getLocalKeys(prepareCommand.getModifications());
               prepareResponseCommand.setKeysValidated(keysValidated);
            }
            try {
               prepareResponseCommand.acceptVisitor(txInvocationContext, invoker);
            } catch (Throwable throwable) {
               //TODO: log this exception
            }
         }
      }
   }

   protected class VersionsCollector {
      private Set<Object> keysMissingVersions;
      private Throwable exceptionObject;
      private boolean alreadyProcessed;

      public VersionsCollector(Collection<Object> keys) {
         keysMissingVersions = new HashSet<Object>(keys);
      }

      public synchronized boolean addKeysValidated(Set<Object> keys) {
         if (alreadyProcessed || keys == null || keys.isEmpty()) {
            return false;
         }
         keysMissingVersions.removeAll(keys);
         return (alreadyProcessed = keysMissingVersions.isEmpty());
      }

      public synchronized boolean addException(Throwable exceptionObject) {
         if (alreadyProcessed) {
            return false;
         }
         this.exceptionObject = exceptionObject;
         alreadyProcessed = true;
         return true;
      }

      public synchronized Throwable getException() {
         return exceptionObject;
      }
   }

   private Set<Object> getLocalKeys(WriteCommand... modifications) {
      Set<Object> localKeys = new HashSet<Object>(modifications.length);

      for (WriteCommand wc : modifications) {
         for (Object key : wc.getAffectedKeys()) {
            if (distributionManager.getLocality(key).isLocal()) {
               localKeys.add(key);
            }
         }
      }
      
      return localKeys;
   }

   private Set<Object> getModifiedKeys(Collection<WriteCommand> modifications) {
      Set<Object> modifiedKeys = new HashSet<Object>();
      for (WriteCommand wc : modifications) {
         modifiedKeys.addAll(wc.getAffectedKeys());
      }
      return modifiedKeys;
   }
}
