package org.infinispan.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.totalOrder.TotalOrderRemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Units;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mircea.markus@jboss.com
 * @since 5.2.0
 */
public abstract class BaseTotalOrderManager implements TotalOrderManager {

   protected final Log log = LogFactory.getLog(getClass());

   protected Configuration configuration;
   protected InvocationContextContainer invocationContextContainer;
   protected TransactionTable transactionTable;

   protected boolean trace;

   protected final AtomicLong validationDuration = new AtomicLong(0);
   protected final AtomicInteger numberOfTxValidated = new AtomicInteger(0);

   /**
    * Map between GlobalTransaction and LocalTransaction. used to sync the threads in remote validation and the
    * transaction execution thread.
    */
   private final ConcurrentMap<GlobalTransaction, LocalTransaction> localTransactionMap =
         new ConcurrentHashMap<GlobalTransaction, LocalTransaction>();


   /**
    * Volatile as its value can be changed by a JMX thread.
    */
   protected volatile boolean statisticsEnabled;


   protected final void updateLocalTransaction(Object result, boolean exception, PrepareCommand prepareCommand) {
      GlobalTransaction gtx = prepareCommand.getGlobalTransaction();
      LocalTransaction localTransaction = localTransactionMap.get(gtx);

      if (localTransaction != null) {
         localTransaction.addPrepareResult(result, exception);
         localTransactionMap.remove(gtx);
      } else {
         log.tracef("There's no local transaction corresponding to this(%s) remote transaction", gtx);
      }
   }

   public final void addLocalTransaction(GlobalTransaction globalTransaction, LocalTransaction localTransaction) {
      if (trace)
         log.tracef("Receiving local prepare command. Transaction is %s", globalTransaction.prettyPrint());
      localTransactionMap.put(globalTransaction, localTransaction);
   }


   @Inject
   public void inject(Configuration configuration, InvocationContextContainer invocationContextContainer,
                      TransactionTable transactionTable) {
      this.configuration = configuration;
      this.invocationContextContainer = invocationContextContainer;
      this.transactionTable = transactionTable;
   }

   @Start
   public void start() {
      trace = log.isTraceEnabled();
      setStatisticsEnabled(configuration.jmxStatistics().enabled());
   }

   @Override
   public final void finishTransaction(GlobalTransaction gtx, boolean ignoreNullTxInfo) {
      if (trace) log.tracef("transaction %s is finished", gtx.prettyPrint());

      TotalOrderRemoteTransaction remoteTransaction = (TotalOrderRemoteTransaction) transactionTable.removeRemoteTransaction(gtx);
      if (remoteTransaction != null) {
         finishTransaction(remoteTransaction);
      } else if (!ignoreNullTxInfo) {
         log.remoteTransactionIsNull(gtx.prettyPrint());
      }
   }

   @Override
   public final boolean waitForTxPrepared(TotalOrderRemoteTransaction remoteTransaction, boolean commit,
                                    EntryVersionsMap newVersions) {
      GlobalTransaction gtx = remoteTransaction.getGlobalTransaction();
      if (trace)
         log.tracef("Waiting until transaction %s is prepared. New versions are %s", gtx.prettyPrint(), newVersions);

      boolean needsToProcessCommand = false;
      try {
         needsToProcessCommand = remoteTransaction.waitPrepared(commit, newVersions);
      } catch (InterruptedException e) {
         log.timeoutWaitingUntilTransactionPrepared(gtx.prettyPrint());
         needsToProcessCommand = false;
      } finally {
         if (trace) log.tracef("Transaction %s finishes the waiting time", gtx.prettyPrint());
      }
      return needsToProcessCommand;
   }

   @Override
   public void finishTransaction(TotalOrderRemoteTransaction remoteTransaction) {
      TxDependencyLatch latch = remoteTransaction.getLatch();
      if (trace)  log.tracef("Releasing resources for transaction %s", remoteTransaction);
      latch.countDown();
   }


   @ManagedAttribute(description = "Average duration of a transaction validation (milliseconds)")
   @Metric(displayName = "Average Validation Duration", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
   public double getAverageValidationDuration() {
      long time = validationDuration.get();
      int tx = numberOfTxValidated.get();
      if (tx == 0) {
         return 0;
      }
      return (time / tx) / 1000000.0;
   }

   @ManagedOperation(description = "Resets the statistics")
   public void resetStatistics() {
      validationDuration.set(0);
      numberOfTxValidated.set(0);
   }

   @ManagedAttribute(description = "Show it the gathering of statistics is enabled")
   public boolean isStatisticsEnabled() {
      return statisticsEnabled;
   }

   @ManagedOperation(description = "Enables or disables the gathering of statistics by this component")
   public void setStatisticsEnabled(boolean statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
   }

   protected final long now() {
      //we know that this is only used for stats
      return statisticsEnabled ? System.nanoTime() : -1;
   }
}
