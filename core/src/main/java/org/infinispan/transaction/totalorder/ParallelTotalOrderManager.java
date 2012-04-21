package org.infinispan.transaction.totalorder;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.executors.ControllableExecutorService;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Units;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.factories.KnownComponentNames.TOTAL_ORDER_EXECUTOR;

/**
 * @author Pedro Ruivo
 * @author Mircea.markus@jboss.org
 * @since 5.2
 */
@MBean(objectName = "TotalOrderManager", description = "Concurrent total order management")
public class ParallelTotalOrderManager extends BaseTotalOrderManager {

   private static final Log log = LogFactory.getLog(ParallelTotalOrderManager.class);

   private final AtomicLong waitTimeInQueue = new AtomicLong(0);
   private final AtomicLong initializationDuration = new AtomicLong(0);

   /**
    * this map is used to keep track of concurrent transactions.
    */
   private final ConcurrentMap<Object, TxDependencyLatch> keysLocked = new ConcurrentHashMap<Object, TxDependencyLatch>();

   private volatile ExecutorService validationExecutorService;
   private volatile boolean controllableExecutorService;

   @Inject
   public void inject(@ComponentName(TOTAL_ORDER_EXECUTOR) ExecutorService e) {
      validationExecutorService = e;
      controllableExecutorService = validationExecutorService instanceof ControllableExecutorService;
   }

   @Override
   public final void processTransactionFromSequencer(PrepareCommand prepareCommand, TxInvocationContext ctx,
                                                     CommandInterceptor invoker) {
      logAndCheckContext(prepareCommand, ctx);
      
      copyLookedUpEntriesToRemoteContext(ctx);

      TotalOrderRemoteTransaction remoteTransaction = (TotalOrderRemoteTransaction) ctx.getCacheTransaction();

      ParallelPrepareProcessor ppp = new ParallelPrepareProcessor(prepareCommand, ctx, invoker, remoteTransaction);
      Set<TxDependencyLatch> previousTxs = new HashSet<TxDependencyLatch>();

      //this will collect all the count down latch corresponding to the previous transactions in the queue
      for (Object key : remoteTransaction.getModifiedKeys()) {
         TxDependencyLatch prevTx = keysLocked.put(key, remoteTransaction.getLatch());
         if (prevTx != null) {
            previousTxs.add(prevTx);
         }
      }

      ppp.setPreviousTransactions(previousTxs);

      if (trace)
         log.tracef("Transaction [%s] write set is %s", remoteTransaction.getLatch(), remoteTransaction.getModifiedKeys());

      validationExecutorService.execute(ppp);
   }

   @Override
   public final void finishTransaction(TotalOrderRemoteTransaction remoteTransaction) {
      super.finishTransaction(remoteTransaction);
      for (Object key : remoteTransaction.getModifiedKeys()) {
         this.keysLocked.remove(key, remoteTransaction.getLatch());
      }
   }

   protected ParallelPrepareProcessor buildMultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                    CommandInterceptor invoker, TotalOrderRemoteTransaction remoteTransaction) {
      return new ParallelPrepareProcessor(prepareCommand, txInvocationContext, invoker, remoteTransaction);
   }

   /**
    * This class is used to validate transaction in repeatable read with write skew check
    */
   protected class ParallelPrepareProcessor implements Runnable {

      //the set of others transaction's count down latch (it will be unblocked when the transaction finishes)
      private final Set<TxDependencyLatch> previousTransactions;

      protected TotalOrderRemoteTransaction remoteTransaction = null;

      protected final PrepareCommand prepareCommand;
      protected final TxInvocationContext txInvocationContext;
      protected final CommandInterceptor invoker;

      private long creationTime = -1;
      private long processStartTime = -1;
      private long initializationEndTime = -1;

      protected ParallelPrepareProcessor(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                       CommandInterceptor invoker, TotalOrderRemoteTransaction remoteTransaction) {
         if (prepareCommand == null || txInvocationContext == null || invoker == null) {
            throw new IllegalArgumentException("Arguments must not be null");
         }
         this.prepareCommand = prepareCommand;
         this.txInvocationContext = txInvocationContext;
         this.invoker = invoker;
         this.creationTime = now();
         this.previousTransactions = new HashSet<TxDependencyLatch>();
         this.remoteTransaction = remoteTransaction;
      }

      public void setPreviousTransactions(Set<TxDependencyLatch> previousTransactions) {
         this.previousTransactions.addAll(previousTransactions);
      }

      /**
       * set the initialization of the thread before the validation ensures the validation order in conflicting
       * transactions
       *
       * @throws InterruptedException if this thread was interrupted
       */
      protected void initializeValidation() throws Exception {
         String gtx = prepareCommand.getGlobalTransaction().prettyPrint();
         //todo is this really needed?
         invocationContextContainer.setContext(txInvocationContext);

         /*
         we need to ensure the order before cancelling the transaction, because of this scenario:

         Tx2 receives a rollback command
         Tx1 is deliver and touch Key_X
         Tx1 is blocked (ensure the order)
         Tx2 is deliver and touch Key_X (and saves the latch of Tx1)
         Tx3 is deliver and touch Key_X (and saves the latch of Tx2)
         Tx2 is immediately aborted (already received the rollback) and releases the latch
         Tx3 commits and writes in Key_X
         Tx1 later aborts (in the other nodes, Tx1 commits and Tx3 aborts)
         */
         //if (remoteTransaction.isMarkedForRollback()) {
         //   throw new CacheException("Cannot prepare transaction" + gtx + ". it was already marked as rollback");
         //} 

         boolean isResend = prepareCommand.isOnePhaseCommit();
         if (isResend) {
            previousTransactions.remove(remoteTransaction.getLatch());
         } else if (previousTransactions.contains(remoteTransaction.getLatch())) {
            throw new IllegalStateException("Dependency transaction must not contains myself in the set");
         }

         for (TxDependencyLatch prevTx : previousTransactions) {
            if (trace) log.tracef("Transaction %s will wait for %s", gtx, prevTx);
            prevTx.await();
         }

         remoteTransaction.markForPreparing();

         if (remoteTransaction.isMarkedForRollback()) {
            //this means that rollback has already been received
            transactionTable.removeRemoteTransaction(remoteTransaction.getGlobalTransaction());
            throw new CacheException("Cannot prepare transaction" + gtx + ". it was already marked as rollback");
         }

         if (remoteTransaction.isMarkedForCommit()) {
            log.tracef("Transaction %s marked for commit, skipping the write skew check and forcing 1PC", gtx);
            txInvocationContext.setFlags(Flag.SKIP_WRITE_SKEW_CHECK);
            prepareCommand.setOnePhaseCommit(true);
         }
      }

      @Override
      public void run() {
         processStartTime = now();
         Object result = null;
         boolean exception = false;
         try {
            if (trace) log.tracef("Validating transaction %s ",
                                  prepareCommand.getGlobalTransaction().prettyPrint());


            initializeValidation();
            initializationEndTime = now();

            //invoke next interceptor in the chain
            result = prepareCommand.acceptVisitor(txInvocationContext, invoker);
         } catch (Throwable t) {
            log.trace("Exception while processing the rest of the interceptor chain", t);
            if (initializationEndTime == -1) {
               initializationEndTime = now();
            }
            result = t;
            exception = true;
         } finally {
            if (trace)
               log.tracef("Transaction %s finished validation (%s). Validation result is %s ",
                          prepareCommand.getGlobalTransaction().prettyPrint(),
                          (exception ? "failed" : "ok"), (exception ? ((Throwable) result).getMessage() : result));

            finalizeProcessing(result, exception);
            updateDurationStats(creationTime, processStartTime, now(), initializationEndTime);
         }
      }

      /**
       * finishes the transaction, ie, mark the modification as applied and set the result (exception or not) invokes
       * the method #finishTransaction if the transaction has the one phase commit set to true
       */
      protected void finalizeProcessing(Object result, boolean exception) {
         remoteTransaction.markPreparedAndNotify();
         updateLocalTransaction(result, exception, prepareCommand.getGlobalTransaction());
         if (prepareCommand.isOnePhaseCommit() || exception) {
            markTxCompleted();
         }
      }

      private void markTxCompleted() {
         finishTransaction(remoteTransaction);
         transactionTable.removeRemoteTransaction(prepareCommand.getGlobalTransaction());
      }
   }

   /**
    * updates the accumulating time for profiling information
    *
    * @param creationTime          the arrival timestamp of the prepare command to this component in remote
    * @param validationStartTime   the processing start timestamp
    * @param validationEndTime     the validation ending timestamp
    * @param initializationEndTime the initialization ending timestamp
    */
   private void updateDurationStats(long creationTime, long validationStartTime, long validationEndTime,
                                    long initializationEndTime) {
      if (statisticsEnabled) {
         //set the profiling information
         waitTimeInQueue.addAndGet(validationStartTime - creationTime);
         initializationDuration.addAndGet(initializationEndTime - validationStartTime);
         processingDuration.addAndGet(validationEndTime - initializationEndTime);
         numberOfTxValidated.incrementAndGet();
      }
   }

   @ManagedOperation(description = "Resets the statistics")
   public void resetStatistics() {
      super.resetStatistics();
      waitTimeInQueue.set(0);
      initializationDuration.set(0);
   }


   @ManagedAttribute(description = "The minimum number of threads in the thread pool")
   @Metric(displayName = "Minimum Number of Threads", displayType = DisplayType.DETAIL)
   public int getThreadPoolCoreSize() {
      if (controllableExecutorService) {
         return ((ControllableExecutorService) validationExecutorService).getCorePoolSize();
      } else {
         return 1;
      }
   }

   @ManagedAttribute(description = "The maximum number of threads in the thread pool")
   @Metric(displayName = "Maximum Number of Threads", displayType = DisplayType.DETAIL)
   public int getThreadPoolMaximumPoolSize() {
      if (controllableExecutorService) {
         return ((ControllableExecutorService) validationExecutorService).getMaximumPoolSize();
      } else {
         return 1;
      }
   }

   @ManagedAttribute(description = "The keep alive time of an idle thread in the thread pool (milliseconds)")
   @Metric(displayName = "Keep Alive Time of a Idle Thread", units = Units.MILLISECONDS,
           displayType = DisplayType.DETAIL)
   public long getThreadPoolKeepTime() {
      if (controllableExecutorService) {
         return ((ControllableExecutorService) validationExecutorService).getKeepAliveTime();
      } else {
         return 0;
      }
   }

   @ManagedAttribute(description = "The percentage of occupation of the queue")
   @Metric(displayName = "Percentage of Occupation of the Queue", units = Units.PERCENTAGE,
           displayType = DisplayType.SUMMARY)
   public double getNumberOfTransactionInPendingQueue() {
      if (controllableExecutorService) {
         return ((ControllableExecutorService) validationExecutorService).getQueueOccupationPercentage();
      } else {
         return 0D;
      }
   }

   @ManagedAttribute(description = "The approximate percentage of active threads in the thread pool")
   @Metric(displayName = "Percentage of Active Threads", units = Units.PERCENTAGE, displayType = DisplayType.SUMMARY)
   public double getPercentageActiveThreads() {
      if (controllableExecutorService) {
         return ((ControllableExecutorService) validationExecutorService).getUsagePercentage();
      } else {
         return 0D;
      }
   }

   @ManagedAttribute(description = "Average time in the queue before the validation (milliseconds)")
   @Metric(displayName = "Average Waiting Duration In Queue", units = Units.MILLISECONDS,
           displayType = DisplayType.SUMMARY)
   public double getAverageWaitingTimeInQueue() {
      long time = waitTimeInQueue.get();
      int tx = numberOfTxValidated.get();
      if (tx == 0) {
         return 0;
      }
      return (time / tx) / 1000000.0;
   }

   @ManagedAttribute(description = "Average duration of a transaction initialization before validation, ie, " +
         "ensuring the order of transactions (milliseconds)")
   @Metric(displayName = "Average Initialization Duration", units = Units.MILLISECONDS,
           displayType = DisplayType.SUMMARY)
   public double getAverageInitializationDuration() {
      long time = initializationDuration.get();
      int tx = numberOfTxValidated.get();
      if (tx == 0) {
         return 0;
      }
      return (time / tx) / 1000000.0;
   }

   @ManagedOperation(description = "Set the minimum number of threads in the thread pool")
   @Operation(displayName = "Set Minimum Number Of Threads")
   public void setThreadPoolCoreSize(int size) {
      if (controllableExecutorService) {
         ((ControllableExecutorService) validationExecutorService).setCorePoolSize(size);
      }
   }

   @ManagedOperation(description = "Set the maximum number of threads in the thread pool")
   @Operation(displayName = "Set Maximum Number Of Threads")
   public void setThreadPoolMaximumPoolSize(int size) {
      if (controllableExecutorService) {
         ((ControllableExecutorService) validationExecutorService).setMaximumPoolSize(size);
      }
   }

   @ManagedOperation(description = "Set the idle time of a thread in the thread pool (milliseconds)")
   @Operation(displayName = "Set Keep Alive Time of Idle Threads")
   public void setThreadPoolKeepTime(long time) {
      if (controllableExecutorService) {
         ((ControllableExecutorService) validationExecutorService).setKeepAliveTime(time);
      }
   }
}
