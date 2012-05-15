package org.infinispan.distribution.wrappers;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.container.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.lang.reflect.Field;

/**
 * Massive hack for a noble cause!
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "ExtendedStatistics", description = "Component that manages and exposes extended statistics " +
      "relevant to transactions.")
public abstract class CustomStatsInterceptor extends BaseCustomInterceptor {
   //TODO what about the transaction implicit vs transaction explicit? should we take in account this and ignore
   //the implicit stuff?

   private final Log log = LogFactory.getLog(getClass());

   private TransactionTable transactionTable;

   @Inject
   public void inject(TransactionTable transactionTable) {
      this.transactionTable = transactionTable;
   }

   @Start
   public void start(){
      replace();
      log.warn("Initializing the TransactionStatisticsRegistry");
      TransactionsStatisticsRegistry.init();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      log.tracef("Visit Put Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                 ctx.isInTxScope(), ctx.isOriginLocal());
      Object ret;
      if(ctx.isInTxScope()){
         this.initStatsIfNecessary(ctx);
         TransactionsStatisticsRegistry.setUpdateTransaction();
         long currTime = System.nanoTime();
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_PUTS);
         try {
            ret =  invokeNextInterceptor(ctx,command);
         } catch (TimeoutException e) {
            if (ctx.isOriginLocal() && isLockTimeout(e)) {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_TIMEOUT);
            }
            throw e;
         } catch (DeadlockDetectedException e) {
            if (ctx.isOriginLocal()) {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_DEADLOCK);
            }
            throw e;
         }
         if(isRemote(command.getKey())){
            TransactionsStatisticsRegistry.addValue(IspnStats.REMOTE_PUT_EXECUTION,System.nanoTime() - currTime);
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTE_PUT);
         }
         return ret;
      }
      else
         return invokeNextInterceptor(ctx,command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable{
      log.tracef("Visit Get Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                 ctx.isInTxScope(), ctx.isOriginLocal());
      boolean isTx = ctx.isInTxScope();
      Object ret;
      if(isTx){
         this.initStatsIfNecessary(ctx);
         long currTime = 0;
         boolean isRemoteKey = isRemote(command.getKey());
         if(isRemoteKey){
            currTime = System.nanoTime();
         }

         ret = invokeNextInterceptor(ctx,command);
         if(isRemoteKey){
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTE_GET);
            TransactionsStatisticsRegistry.addValue(IspnStats.REMOTE_GET_EXECUTION, System.nanoTime() - currTime);
         }
      }
      else{
         ret = invokeNextInterceptor(ctx,command);
      }
      return ret;
   }

   protected boolean isRemote(Object key){
      return false;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      log.tracef("Visit Commit command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      long currTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx,command);
      updateTime(IspnStats.COMMIT_EXECUTION_TIME, IspnStats.NUM_COMMIT_COMMAND, currTime);
      TransactionsStatisticsRegistry.setTransactionOutcome(true);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction();
      }
      return ret;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      log.tracef("Visit Prepare command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      TransactionsStatisticsRegistry.onPrepareCommand();
      if (command.hasModifications()) {
         TransactionsStatisticsRegistry.setUpdateTransaction();
      }

      try {
         long currTime = System.nanoTime();
         Object ret = invokeNextInterceptor(ctx,command);
         updateTime(IspnStats.PREPARE_EXECUTION_TIME, IspnStats.NUM_PREPARE_COMMAND, currTime);
         return ret;
      } catch (TimeoutException e) {
         if (ctx.isOriginLocal() && isLockTimeout(e)) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_TIMEOUT);
         }
         throw e;
      } catch (DeadlockDetectedException e) {
         if (ctx.isOriginLocal()) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_DEADLOCK);
         }
         throw e;
      }
   }


   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable{
      log.tracef("Visit Rollback command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      long initRollbackTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx,command);
      updateTime(IspnStats.ROLLBACK_EXECUTION_TIME, IspnStats.NUM_ROLLBACKS, initRollbackTime);
      TransactionsStatisticsRegistry.setTransactionOutcome(false);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction();
      }
      return ret;
   }

   private void replace(){
      log.infof("CustomStatsInterceptor Enabled!");
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();

      GlobalComponentRegistry globalComponentRegistry = componentRegistry.getGlobalComponentRegistry();
      InboundInvocationHandlerWrapper invocationHandlerWrapper = rewireInvocationHandler(globalComponentRegistry);
      globalComponentRegistry.rewire();

      replaceFieldInTransport(componentRegistry, invocationHandlerWrapper);

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      replaceEntryFactoryWrapper(componentRegistry);
      componentRegistry.rewire();
   }

   private void replaceFieldInTransport(ComponentRegistry componentRegistry, InboundInvocationHandlerWrapper invocationHandlerWrapper) {
      JGroupsTransport t = (JGroupsTransport) componentRegistry.getComponent(Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      try {
         Field f = card.getClass().getDeclaredField("inboundInvocationHandler");
         f.setAccessible(true);
         f.set(card, invocationHandlerWrapper);
      } catch (NoSuchFieldException e) {
         e.printStackTrace();
      } catch (IllegalAccessException e) {
         e.printStackTrace();
      }
   }

   private InboundInvocationHandlerWrapper rewireInvocationHandler(GlobalComponentRegistry globalComponentRegistry) {
      InboundInvocationHandler inboundHandler = globalComponentRegistry.getComponent(InboundInvocationHandler.class);
      InboundInvocationHandlerWrapper invocationHandlerWrapper = new InboundInvocationHandlerWrapper(inboundHandler,
                                                                                                     transactionTable);
      globalComponentRegistry.registerComponent(invocationHandlerWrapper, InboundInvocationHandler.class);
      return invocationHandlerWrapper;
   }

   private void replaceEntryFactoryWrapper(ComponentRegistry componentRegistry) {
      EntryFactory entryFactory = componentRegistry.getComponent(EntryFactory.class);
      EntryFactoryWrapper entryFactoryWrapper = new EntryFactoryWrapper(entryFactory);
      componentRegistry.registerComponent(entryFactoryWrapper, EntryFactory.class);
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager lockManager = componentRegistry.getComponent(LockManager.class);
      LockManagerWrapper lockManagerWrapper = new LockManagerWrapper(lockManager);
      componentRegistry.registerComponent(lockManagerWrapper, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      RpcManagerWrapper rpcManagerWrapper = new RpcManagerWrapper(rpcManager);
      componentRegistry.registerComponent(rpcManagerWrapper, RpcManager.class);
   }

   private void initStatsIfNecessary(InvocationContext ctx){
      if(ctx.isInTxScope())
         TransactionsStatisticsRegistry.initTransactionIfNecessary((TxInvocationContext) ctx);
   }

   private boolean isLockTimeout(TimeoutException e) {
      return e.getMessage().startsWith("Unable to acquire lock after");
   }

   private void updateTime(IspnStats duration, IspnStats counter, long initTime) {
      TransactionsStatisticsRegistry.addValue(duration, System.nanoTime() - initTime);
      TransactionsStatisticsRegistry.incrementValue(counter);
   }

   //JMX exposed methods

   @ManagedAttribute(description = "Average number of puts performed locally by a successful local transaction")
   @Metric(displayName = "Number of puts")
   public long getAvgNumPutsBySuccessfulLocalTx(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PUTS_PER_LOCAL_TX);
   }

   @ManagedAttribute(description = "Average Prepare RTT duration")
   @Metric(displayName = "Average Prepare RTT")
   public long getAvgPrepareRtt() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_PREPARE)));
   }

   @ManagedAttribute(description = "Average Commit RTT duration")
   @Metric(displayName = "Average Commit RTT")
   public long getAvgCommitRtt() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_COMMIT)));
   }

   @ManagedAttribute(description = "Average Remote Get RTT duration")
   @Metric(displayName = "Average Remote Get RTT")
   public long getAvgRemoteGetRtt() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_GET)));
   }

   @ManagedAttribute(description = "Average Rollback RTT duration")
   @Metric(displayName = "Average Rollback RTT")
   public long getAvgRollbackRtt() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_ROLLBACK)));
   }

   @ManagedAttribute(description = "Average Prepare asynchronous duration")
   @Metric(displayName = "Average Prepare Async")
   public long getAvgPrepareAsync() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_PREPARE)));
   }

   @ManagedAttribute(description = "Average Commit asynchronous duration")
   @Metric(displayName = "Average Commit Async")
   public long getAvgCommitAsync() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMMIT)));
   }

   @ManagedAttribute(description = "Average Complete Notification asynchronous duration")
   @Metric(displayName = "Average Complete Notification Async")
   public long getAvgCompleteNotificationAsync() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMPLETE_NOTIFY)));
   }

   @ManagedAttribute(description = "Average Rollback asynchronous duration")
   @Metric(displayName = "Average Rollback Async")
   public long getAvgRollbackAsync() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_ROLLBACK)));
   }

   @ManagedAttribute(description = "Average number of nodes in Commit destination set")
   @Metric(displayName = "Average Number of Nodes in Commit Destination Set")
   public long getAvgNumNodesCommit() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMMIT)));
   }

   @ManagedAttribute(description = "Average number of nodes in Complete Notification destination set")
   @Metric(displayName = "Average Number of Nodes in Complete Notification Destination Set")
   public long getAvgNumNodesCompleteNotification() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMPLETE_NOTIFY)));
   }

   @ManagedAttribute(description = "Average number of nodes in Remote Get destination set")
   @Metric(displayName = "Average Number of Nodes in Remote Get Destination Set")
   public long getAvgNumNodesRemoteGet() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_GET)));
   }

   @ManagedAttribute(description = "Average number of nodes in Prepare destination set")
   @Metric(displayName = "Average Number of Nodes in Prepare Destination Set")
   public long getAvgNumNodesPrepare() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_PREPARE)));
   }

   @ManagedAttribute(description = "Average number of nodes in Rollback destination set")
   @Metric(displayName = "Average Number of Nodes in Rollback Destination Set")
   public long getAvgNumNodesRollback() {
      return (Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_ROLLBACK)));
   }

   @ManagedAttribute(description = "Application Contention Factor")
   @Metric(displayName = "Application Contention Factor")
   public double getApplicationContentionFactor() {
      return (Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.APPLICATION_CONTENTION_FACTOR));
   }

   @Deprecated
   @ManagedAttribute(description = "Local Contention Probability")
   @Metric(displayName = "Local Conflict Probability")
   public double getLocalContentionProbability(){
      return (Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_CONTENTION_PROBABILITY));
   }

   @Deprecated
   @ManagedAttribute(description = "Remote Contention Probability")
   @Metric(displayName = "Remote Conflict Probability")
   public double getRemoteContentionProbability(){
      return (Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.REMOTE_CONTENTION_PROBABILITY));
   }

   @ManagedAttribute(description = "Lock Contention Probability")
   @Metric(displayName = "Lock Contention Probability")
   public double getLockContentionProbability(){
      return (Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCK_CONTENTION_PROBABILITY));
   }


   @ManagedAttribute(description = "Local execution time of a transaction without the time waiting for lock acquisition")
   @Metric(displayName = "Local Execution Time Without Locking Time")
   public long getLocalExecutionTimeWithoutLock(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_EXEC_NO_CONT);
   }

   @ManagedAttribute(description = "Average lock holding time")
   @Metric(displayName = "Average Lock Holding Time")
   public long getAvgLockHoldTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME);
   }

   @ManagedAttribute(description = "Average lock local holding time")
   @Metric(displayName = "Average Lock Local Holding Time")
   public long getAvgLocalLockHoldTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_LOCAL);
   }

   @ManagedAttribute(description = "Average lock remote holding time")
   @Metric(displayName = "Average Lock Remote Holding Time")
   public long getAvgRemoteLockHoldTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_REMOTE);
   }

   @ManagedAttribute(description = "Average commit duration time (2nd phase only)")
   @Metric(displayName = "Average Commit Time")
   public long getAvgCommitTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average rollback duration time (2nd phase only)")
   @Metric(displayName = "Average Rollback Time")
   public long getAvgRollbackTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.ROLLBACK_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average prepare command size")
   @Metric(displayName = "Average Prepare Command Size")
   public long getAvgPrepareCommandSize(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PREPARE_COMMAND_SIZE);
   }

   @ManagedAttribute(description = "Average commit command size")
   @Metric(displayName = "Average Commit Command Size")
   public long getAvgCommitCommandSize(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_COMMAND_SIZE);
   }

   @ManagedAttribute(description = "Average clustered get command size")
   @Metric(displayName = "Average Clustered Get Command Size")
   public long getAvgClusteredGetCommandSize(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.CLUSTERED_GET_COMMAND_SIZE);
   }

   @ManagedAttribute(description = "Average time waiting for the lock acquisition")
   @Metric(displayName = "Average Lock Waiting Time")
   public long getAvgLockWaitingTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_WAITING_TIME);
   }

   @ManagedAttribute(description = "Average transaction arrival rate")
   @Metric(displayName = "Average Transaction Arrival Rate")
   public long getAvgTxArrivalRate(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.ARRIVAL_RATE);
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed locally (committed and aborted)")
   @Metric(displayName = "Percentage of Write Transactions")
   public double getPercentageWriteTransactions(){
      return (Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_WRITE_PERCENTAGE);
   }

   @ManagedAttribute(description = "Percentage of successfully Write transaction executed locally")
   @Metric(displayName = "Percentage of Successfully Write Transactions")
   public double getPercentageSuccessWriteTransactions(){
      return (Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.SUCCESSFUL_WRITE_PERCENTAGE);
   }

   @ManagedAttribute(description = "The number of aborted transactions due to timeout in lock acquisition")
   @Metric(displayName = "Number of Aborted Transaction due to Lock Acquisition Timeout")
   public long getNumAbortedTxDueTimeout(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_TIMEOUT);
   }

   @ManagedAttribute(description = "The number of aborted transactions due to deadlock")
   @Metric(displayName = "Number of Aborted Transaction due to Deadlock")
   public long getNumAbortedTxDueDeadlock(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_DEADLOCK);
   }

   @ManagedAttribute(description = "Average successful read-only transaction duration")
   @Metric(displayName = "Average Read-Only Transaction Duration")
   public long getAvgReadOnlyTxDuration(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average successful write transaction duration")
   @Metric(displayName = "Average Write Transaction Duration")
   public long getAvgWriteTxDuration(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average write transaction local execution time")
   @Metric(displayName = "Average Write Transaction Local Execution Time")
   public long getAvgWriteTxLocalExecution(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_LOCAL_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average number of locks per write local transaction")
   @Metric(displayName = "Average Number of Lock per Local Transaction")
   public long getAvgNumOfLockLocalTx(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_LOCAL_TX);
   }

   @ManagedAttribute(description = "Average number of locks per write remote transaction")
   @Metric(displayName = "Average Number of Lock per Remote Transaction")
   public long getAvgNumOfLockRemoteTx(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_REMOTE_TX);
   }

   @ManagedAttribute(description = "Average number of locks per successfully write local transaction")
   @Metric(displayName = "Average Number of Lock per Successfully Local Transaction")
   public long getAvgNumOfLockSuccessLocalTx(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_SUCCESS_LOCAL_TX);
   }

   @ManagedAttribute(description = "Average time it takes to execute the prepare command locally")
   @Metric(displayName = "Average Local Prepare Execution Time")
   public long getAvgLocalPrepareTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_PREPARE_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the prepare command remotely")
   @Metric(displayName = "Average Remote Prepare Execution Time")
   public long getAvgRemotePrepareTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PREPARE_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the commit command locally")
   @Metric(displayName = "Average Local Commit Execution Time")
   public long getAvgLocalCommitTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the commit command remotely")
   @Metric(displayName = "Average Remote Commit Execution Time")
   public long getAvgRemoteCommitTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command locally")
   @Metric(displayName = "Average Local Rollback Execution Time")
   public long getAvgLocalRollbackTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command remotely")
   @Metric(displayName = "Average Remote Rollback Execution Time")
   public long getAvgRemoteRollbackTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command remotely")
   @Metric(displayName = "Average Remote Transaction Completion Notify Execution Time")
   public long getAvgRemoteTxCompleteNotifyTime(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_COMPLETE_NOTIFY_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Abort Rate")
   @Metric(displayName = "Abort Rate")
   public double getAbortRate(){
      return (Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.ABORT_RATE);
   }

   @ManagedAttribute(description = "Throughput")
   @Metric(displayName = "Throughput")
   public long getThroughput(){
      return (Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.THROUGHPUT);
   }

   @ManagedOperation(description = "K-th percentile of local read-only transactions execution time")
   @Operation(displayName = "K-th Percentile Local Read-Only Transactions")
   public double getPercentileLocalReadOnlyTransaction(int percentile){
      return (Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_LOCAL_PERCENTILE, percentile);
   }

   @ManagedOperation(description = "K-th percentile of remote read-only transactions execution time")
   @Operation(displayName = "K-th Percentile Remote Read-Only Transactions")
   public double getPercentileRemoteReadOnlyTransaction(int percentile){
      return (Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_REMOTE_PERCENTILE, percentile);
   }

   @ManagedOperation(description = "K-th percentile of local write transactions execution time")
   @Operation(displayName = "K-th Percentile Local Write Transactions")
   public double getPercentileLocalRWriteTransaction(int percentile){
      return (Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_LOCAL_PERCENTILE, percentile);
   }

   @ManagedOperation(description = "K-th percentile of remote write transactions execution time")
   @Operation(displayName = "K-th Percentile Remote Write Transactions")
   public double getPercentileRemoteWriteTransaction(int percentile){
      return (Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_REMOTE_PERCENTILE, percentile);
   }

   @ManagedOperation(description = "Reset all the statistics collected")
   @Operation(displayName = "Reset All Statistics")
   public void resetStatistics(){
      TransactionsStatisticsRegistry.reset();
   }


}
