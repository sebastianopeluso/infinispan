package org.infinispan.totalorder;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.PrepareResponseCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.totalOrder.TotalOrderRemoteTransaction;
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
public class DistributedTotalOrderManager extends TotalOrderManager {

   private CommandsFactory commandsFactory;
   private ConcurrentMap<GlobalTransaction, VersionsCollector> versionsCollectorMap =
         new ConcurrentHashMap<GlobalTransaction, VersionsCollector>();

   @Inject
   public void inject(CommandsFactory commandsFactory) {
      this.commandsFactory = commandsFactory;
   }

   @Override
   public void addLocalTransaction(GlobalTransaction globalTransaction, LocalTransaction localTransaction) {
      super.addLocalTransaction(globalTransaction, localTransaction);
      Set<Object> keys = new HashSet<Object>();

      for (WriteCommand writeCommand : localTransaction.getModifications()) {
         keys.addAll(writeCommand.getAffectedKeys());
      }

      versionsCollectorMap.put(globalTransaction, new VersionsCollector(keys));
   }

   @Override
   public void finishTransaction(GlobalTransaction gtx, boolean ignoreNullTxInfo, 
                                 TotalOrderRemoteTransaction transaction) {
      super.finishTransaction(gtx, ignoreNullTxInfo, transaction);
      versionsCollectorMap.remove(gtx);
   }

   @Override
   public void addVersions(GlobalTransaction gtx, Object result, boolean exception) {
      VersionsCollector versionsCollector = versionsCollectorMap.get(gtx);
      if (versionsCollector != null) {
         if (exception) {
            if (versionsCollector.addException(result)) {
               notifyLocalTransaction(gtx, versionsCollector.getFinalResult(), true);
            }
         } else if (result instanceof EntryVersionsMap) {
            if (versionsCollector.addVersions((EntryVersionsMap) result)) {
               notifyLocalTransaction(gtx, versionsCollector.getFinalResult(), false);
            }
         } else {
            //must not happen!!
         }
      } else {
         //transaction result is already known
      }
   }

   @Override
   protected MultiThreadValidation createMultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext ctx,
                                                               CommandInterceptor invoker, 
                                                               TotalOrderRemoteTransaction totalOrderRemoteTransaction) {
      return new DistMultiThreadValidation(prepareCommand, ctx, invoker, totalOrderRemoteTransaction);
   }

   protected class DistMultiThreadValidation extends MultiThreadValidation {

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
               if (versionsCollector.addException(result)) {
                  notifyLocalTransaction(gtx, versionsCollector.getFinalResult(), true);
               }
            } else if (result instanceof EntryVersionsMap) {
               if (versionsCollector.addVersions((EntryVersionsMap) result)) {
                  notifyLocalTransaction(gtx, versionsCollector.getFinalResult(), false);
               }
            } else {
               //must not happen!!
            }
         } else {
            //send the response
            PrepareResponseCommand prepareResponseCommand = commandsFactory.buildPrepareResponseCommand(gtx);
            if (exception) {
               prepareResponseCommand.addException(result);
            } else {
               prepareResponseCommand.addVersions((EntryVersionsMap) result);
            }
            try {
               prepareResponseCommand.acceptVisitor(txInvocationContext, invoker);
            } catch (Throwable throwable) {
               //log this exception
            }
         }
      }
   }

   protected class VersionsCollector {
      private Set<Object> keysMissingVersions;
      private EntryVersionsMap finalVersions;
      private boolean exception;
      private Object exceptionObject;
      private boolean alreadyProcessed;

      public VersionsCollector(Collection<Object> keys) {
         keysMissingVersions = new HashSet<Object>(keys);
         finalVersions = new EntryVersionsMap();
      }

      public synchronized boolean addVersions(EntryVersionsMap newVersions) {
         if (alreadyProcessed) {
            return false;
         }
         keysMissingVersions.removeAll(newVersions.keySet());
         finalVersions.putAll(newVersions);
         alreadyProcessed = keysMissingVersions.isEmpty();
         return alreadyProcessed;
      }

      public synchronized boolean addException(Object exceptionObject) {
         if (alreadyProcessed) {
            return false;
         }
         this.exception = true;
         this.exceptionObject = exceptionObject;
         keysMissingVersions.clear();
         alreadyProcessed = true;
         return true;
      }

      public synchronized Object getFinalResult() {
         return exception ? exceptionObject : finalVersions;
      }
   }
}
