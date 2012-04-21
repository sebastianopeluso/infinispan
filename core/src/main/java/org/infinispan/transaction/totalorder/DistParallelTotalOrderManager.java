package org.infinispan.transaction.totalorder;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.PrepareResponseCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistParallelTotalOrderManager extends ParallelTotalOrderManager {

   private static final Log log = LogFactory.getLog(DistParallelTotalOrderManager.class);

   private CommandsFactory commandsFactory;
   private DistributionManager distributionManager;
   private final ConcurrentMap<GlobalTransaction, AcksCollector> versionsCollectorMap =
         new ConcurrentHashMap<GlobalTransaction, AcksCollector>();

   @Inject
   public void inject(CommandsFactory commandsFactory, DistributionManager distributionManager) {
      this.commandsFactory = commandsFactory;
      this.distributionManager = distributionManager;
   }

   @Override
   protected void afterAddLocalTransaction(GlobalTransaction globalTransaction, LocalTransaction localTransaction) {
      Set<Object> keys = getModifiedKeys(localTransaction.getModifications());

      if (keys.isEmpty()) {
         return;
      }

      AcksCollector collector = new AcksCollector(keys);
      versionsCollectorMap.put(globalTransaction, collector);

      if (trace) {
         log.tracef("Create an Ack Collector %s for transaction %s", collector, globalTransaction.prettyPrint());
      }
   }

   @Override
   protected void afterFinishTransaction(GlobalTransaction globalTransaction) {
      if (trace) {
         log.tracef("Remove the Ack Collector of transaction %s", globalTransaction.prettyPrint());
      }
      versionsCollectorMap.remove(globalTransaction);
   }

   @Override
   public void addVersions(GlobalTransaction gtx, Throwable exception, Set<Object> keysValidated) {
      AcksCollector collector = versionsCollectorMap.get(gtx);
      if (addToAcksCollector(exception, keysValidated, collector, gtx, false)) {
         updateLocalTransaction(collector.getException(), collector.getException() != null, gtx);
      }
   }

   private boolean addToAcksCollector(Throwable exception, Set<Object> keysValidated, AcksCollector collector,
                                      GlobalTransaction gtx, boolean isThePrepareResult) {
      boolean updateLocalTransaction = false;
      if (collector != null) {
         if (exception != null) {
            updateLocalTransaction = collector.addException(exception, isThePrepareResult);

            if (trace) {
               log.tracef("Added an exception [%s] to the acks collector %s. Transaction is %s. Is it ready to " +
                                "update the Local Transaction? %s", exception.getMessage(), collector, gtx.prettyPrint(),
                          updateLocalTransaction ? "yes" : "no");
            }
         } else {
            updateLocalTransaction = collector.addKeysValidated(keysValidated, isThePrepareResult);
            if (trace) {
               log.tracef("Added keys validated [%s] to the acks collector %s. Transaction is %s. Is it ready to " +
                                "update the Local Transaction? %s", keysValidated, collector, gtx.prettyPrint(),
                          updateLocalTransaction ? "yes" : "no");
            }
         }
      } else {
         //can be the case where the transaction is finished
         if (trace) {
            log.tracef("Received an Prepare Response but the Acks collector does not exists. Transaction is %s",
                       gtx.prettyPrint());
         }
      }
      return updateLocalTransaction;
   }

   @Override
   protected ParallelPrepareProcessor buildMultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext ctx,
                                                                 CommandInterceptor invoker,
                                                                 TotalOrderRemoteTransaction totalOrderRemoteTransaction) {
      return new ParallelPrepareProcessor(prepareCommand, ctx, invoker, totalOrderRemoteTransaction);
   }

   private class DistMultiThreadValidation extends ParallelPrepareProcessor {

      private DistMultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                        CommandInterceptor invoker, TotalOrderRemoteTransaction totalOrderRemoteTransaction) {
         super(prepareCommand, txInvocationContext, invoker, totalOrderRemoteTransaction);
      }

      @Override
      protected void finalizeProcessing(Object result, boolean exception) {
         remoteTransaction.markPreparedAndNotify();
         GlobalTransaction gtx = prepareCommand.getGlobalTransaction();

         if (prepareCommand.isOnePhaseCommit() || remoteTransaction.isMarkedForRollback()) {
            //release the count down latch and the keys;
            finishTransaction(remoteTransaction);
            //commit or rollback command will not be received. delete the remote transaction
            transactionTable.removeRemoteTransaction(gtx);
            afterFinishTransaction(gtx);
            return;
         } else if (exception) {
            //release the resources
            finishTransaction(remoteTransaction);
            //don't remote the remote transaction. the rollback will be received later
         }

         AcksCollector collector = versionsCollectorMap.get(gtx);
         if (collector != null) {
            if (addToAcksCollector(exception ? (Throwable) result : null,
                                   exception ? null : getLocalKeys(prepareCommand.getModifications()),
                                   collector, gtx, true)) {
               updateLocalTransaction(collector.getException(), collector.getException() != null, gtx);
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

            if (trace) {
               log.tracef("Send the Prepare Response Command %s back to originator", prepareResponseCommand);
            }

            try {
               prepareResponseCommand.acceptVisitor(txInvocationContext, invoker);
            } catch (Throwable throwable) {
               log.exceptionWhileSendingPrepareResponseCommand(throwable);
            }
         }
      }
   }

   protected class AcksCollector {
      private Set<Object> keysMissingValidation;
      private Throwable exception;
      private boolean txOutcomeReady;
      private boolean prepareProcessed;

      public AcksCollector(Collection<Object> keys) {
         keysMissingValidation = new HashSet<Object>(keys);
         txOutcomeReady = keysMissingValidation.isEmpty();
         prepareProcessed = false;
      }

      public synchronized boolean addKeysValidated(Set<Object> keys, boolean isThePrepareResult) {
         if (notifyLocalTransaction()) {
            return false;
         }
         prepareProcessed = prepareProcessed || isThePrepareResult;

         if (!txOutcomeReady && keys != null && !keys.isEmpty()) {
            keysMissingValidation.removeAll(keys);
            txOutcomeReady = keysMissingValidation.isEmpty();
         }

         return notifyLocalTransaction();
      }

      public synchronized boolean addException(Throwable exception, boolean isThePrepareResult) {
         if (notifyLocalTransaction()) {
            return false;
         }
         prepareProcessed = prepareProcessed || isThePrepareResult;

         if (!txOutcomeReady) {
            this.exception = exception;
            txOutcomeReady = true;
         }

         return prepareProcessed;
      }

      public synchronized Throwable getException() {
         return exception;
      }

      private boolean notifyLocalTransaction() {
         return txOutcomeReady && prepareProcessed;
      }

      @Override
      public synchronized String toString() {
         return "AcksCollector{" +
               "keysMissingValidation=" + keysMissingValidation +
               ", exception=" + exception +
               ", txOutcomeReady=" + txOutcomeReady +
               ", prepareProcessed=" + prepareProcessed +
               '}';
      }
   }

   private Set<Object> getLocalKeys(WriteCommand... modifications) {
      if (modifications == null) {
         return Collections.emptySet();
      }
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
      if (modifications == null) {
         return Collections.emptySet();
      }

      Set<Object> modifiedKeys = new HashSet<Object>();
      for (WriteCommand wc : modifications) {
         modifiedKeys.addAll(wc.getAffectedKeys());
      }
      return modifiedKeys;
   }
}
