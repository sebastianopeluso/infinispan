/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.interceptors;

import eu.cloudtm.rmi.statistics.StatisticsListManager;
import eu.cloudtm.rmi.statistics.ThreadLocalStatistics;
import eu.cloudtm.rmi.statistics.ThreadStatistics;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.Map;


@MBean(objectName = "TLStatistics", description = "Thread Local statistics interceptor")
public class CacheMgmtInterceptor extends JmxStatsCommandInterceptor {

    private static final boolean COMMIT = true;
    private static final boolean ABORT = false;

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        boolean stats = ctx.isOriginLocal() && getStatisticsEnabled();
        if(stats){
            ThreadStatistics is = ThreadLocalStatistics.getInfinispanThreadStats();
            is.terminateLocalExecution();
        }

        Object result;
        try{
            result = invokeNextInterceptor(ctx, command);
        }
        catch(TimeoutException e){
            if(stats){
                ThreadLocalStatistics.getInfinispanThreadStats().incrementTimeoutExceptionOnPrepare();
            }
            throw e;
        }
        catch(DeadlockDetectedException e){
            if(stats)
                ThreadLocalStatistics.getInfinispanThreadStats().incrementDeadlockExceptionOnPrepare();
            throw e;
        }
        return result;
    }

    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand cmd) throws Throwable{
        if(ctx.isInTxScope() &&  getStatisticsEnabled()){
            Object result;
            if(!ctx.isOriginLocal()){
                //SEBA STATS ON REMOTE GET
                long time = System.nanoTime();
                result = invokeNextInterceptor(ctx,cmd);
                time = System.nanoTime() - time;
                ThreadLocalStatistics.getInfinispanThreadStats().addRemoteGetCost(time);
                return result;
            }
            /*
            else if(this.topKEnabled){
                //PEDRO's

                 Object key = cmd.getKey();
                 ThreadLocalStatistics.getInfinispanThreadStats().addRead(key, isRemote(key));
                 return invokeNextInterceptor(ctx,cmd);

            }
            */
            else {
                ThreadLocalStatistics.getInfinispanThreadStats().incrementGet(isRemote(cmd.getKey()));
                return invokeNextInterceptor(ctx,cmd);
            }
        }
        else
            return invokeNextInterceptor(ctx,cmd);
    }

    @Override
    //NB: ORA ANCHE LE RO CHIAMANO IL PREPARE E IL COMMIT COMMAND
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        long t1 = System.nanoTime();
        Object result = invokeNextInterceptor(ctx, command);
        long t2 = System.nanoTime();
        //DIE
        long commitCost = t2-t1;
        if(getStatisticsEnabled()){
            ThreadStatistics is = ThreadLocalStatistics.getInfinispanThreadStats();
            if(ctx.isOriginLocal()){
                is.addCommitCost(commitCost);
            }
            is.flush(COMMIT,ctx.isOriginLocal());
        }

        return result;
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        long t1 = System.nanoTime();
        Object result = invokeNextInterceptor(ctx, command);
        long t2 = System.nanoTime();
        //DIE
        if(getStatisticsEnabled()){

            ThreadStatistics is = ThreadLocalStatistics.getInfinispanThreadStats();
            if(ctx.isOriginLocal()){
                is.addRollbackCost(t2-t1);
            }
            is.flush(ABORT,ctx.isOriginLocal());

        }

        return result;
    }

    @Override
    public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
        long t1 = System.nanoTime();
        //DIE
        if(ctx.isInTxScope() && getStatisticsEnabled()){
            Object key = command.getKey();
            StatisticsListManager.insertInterArrivalSample((long)(t1/1000.0D), key);
            if(ctx.isOriginLocal()){
                ThreadStatistics th = ThreadLocalStatistics.getInfinispanThreadStats();
                th.incrementPuts();
                th.incrementPut(isRemote(key));
                //PEDRO's
                /*
                if(this.topKEnabled){
                    th.addWrite(key, isRemote(key));
                }
                */
            }
        }

        return invokeNextInterceptor(ctx, command);
    }

    protected boolean isRemote(Object key){
        return false;
    }


    /*
    JMX-EXPOSED METHODS
    */

    @ManagedAttribute(description = "Average Rtt duration")
    @Metric(displayName = "Rtt")
    public long getRtt() {
        return StatisticsListManager.getRtt();
    }

    @ManagedAttribute(description = "Application Contention Factor")
    @Metric(displayName = "Application Contention Factor")
    public double getApplicationContentionFactor(){
        return StatisticsListManager.getApplicationContentionFactor();
    }

    @ManagedAttribute(description = "Local Contention Probability")
    @Metric(displayName = "Local Conflict Probability")
    public double getLocalContentionProbability(){
        return StatisticsListManager.getLocalConflictProbability();
    }

    @ManagedAttribute(description = "Maximum time it takes to replicate successful modifications on the cohorts")
    @Metric(displayName = "Max Replay Time")
    public double getMaxReplayTime(){
        return StatisticsListManager.getMaxRemoteExec();
    }

    @ManagedAttribute(description = "Average time it takes to replicate successful modifications on the cohorts")
    @Metric(displayName = "Avg Replay Time")
    public double getAvgReplayTime(){
        return StatisticsListManager.getAvgRemoteExec();
    }

    @ManagedAttribute(description = "Number of transactions which come to the node per second considering also restarted ones")
    @Metric(displayName = "Transactions arrival rate")
    public double getTransactionsArrivalRate(){
        return StatisticsListManager.getTransactionsArrivalRate();
    }

    @ManagedAttribute(description = "Number of locks acquired per second")
    @Metric(displayName = "Locks acquisition rate")
    public double getLocksAcquisitionRate(){
        return StatisticsListManager.getLockAcquisitionRate();
    }

    @ManagedAttribute(description = "Locks acquired by a locally successful transaction")
    @Metric(displayName = "Per transaction acquired locks")
    public double getPerTransactionAcquiredLocks(){
        return StatisticsListManager.getNumPutsPerSuccessfulTransaction();
    }

    @ManagedAttribute(description = "Time spent to process a CommitCommand by the coordinator")
    @Metric(displayName = "CommitCommand Cost")
    public long getCommitCommandCost(){
        return StatisticsListManager.getCommitCommandCost();
    }

    @ManagedAttribute(description = "Time spent to process a RollbackCommand")
    @Metric(displayName = "RollbackCommand Cost")
    public long getRollbackCommandCost(){
        return StatisticsListManager.getRollbackCommandCost();
    }

    @ManagedAttribute(description = "Time spent waiting to acquire a lock")
    @Metric(displayName = "Lock Waiting Time")
    public long getLockWaitingTime(){
        return StatisticsListManager.getWaitingTimeOnLock();
    }

    @ManagedAttribute(description = "Write percentage of committed transactions")
    @Metric(displayName = "Committed transactions write percentage")
    public double getCommittedTransactionsWritePercentage(){
        return StatisticsListManager.getCommittedTransactionsWritePercentage();
    }

    @ManagedAttribute(description = "Write percentage of incoming transactions")
    @Metric(displayName = "Incoming transactions write percentage, whether they commit or not")
    public double getTransactionsWritePercentage(){
        return StatisticsListManager.getTransactionsWritePercentage();
    }

    @ManagedAttribute(description = "Total execution time for an update committed transaction")
    @Metric(displayName = "Update transaction execution time")
    public long getWriteTransactionTotalExecutionTime(){
        return StatisticsListManager.getTotalExec();
    }

    @ManagedAttribute(description = "Execution time for a read-only transaction")
    @Metric(displayName = "Read-only transaction execution time")
    public long getReadOnlyTransactionExecutionTime(){
        return StatisticsListManager.getLocalExecRO();
    }

    @ManagedAttribute(description = "Execution time for the local part of a write transaction")
    @Metric(displayName = "Update transaction local execution time")
    public long getWriteTransactionLocalExecutionTime(){
        return StatisticsListManager.getLocalExec();
    }

    @ManagedAttribute(description = "Execution time for the local part of a write transaction, without considering time spent to acquire locks")
    @Metric(displayName = "Local Exec No Cont")
    public long getLocalExecNoCont(){
        return StatisticsListManager.getLocalExecNoCont();
    }

    @ManagedAttribute(description = "Size of a PrepareCommand")
    @Metric(displayName = "PrepareCommand size")
    public long getPrepareCommandSize(){
        return StatisticsListManager.getPrepareCommandSize();
    }

    @ManagedAttribute(description = "Size of a CommitCommand")
    @Metric(displayName = "CommitCommand size")
    public long getCommitCommandSize(){
        return StatisticsListManager.getCommitCommandSize();
    }

    @ManagedAttribute(description = "Throughput (txs/sec)")
    @Metric(displayName = "Throughput (txs/sec)")
    public long getThroughput(){
        return StatisticsListManager.getThroughput();
    }

    @ManagedAttribute(description = "Hold Time")
    @Metric(displayName = "Hold Time")
    public long getHoldTime(){
        return StatisticsListManager.getHoldTime();
    }

    @ManagedAttribute(description = "Size of a clusteredGetCommand ")
    @Metric(displayName = "ClusteredGetCommand size")
    public long getClusteredGetCommandSize(){
        return StatisticsListManager.getClusteredGetCommandSize();
    }

    @ManagedAttribute(description = "Number of nodes involved in a PrepareCommand ")
    @Metric(displayName = "Nodes involved in PrepareCommand")
    public double getNumNodesInvolvedInPrepare(){
        return StatisticsListManager.getNumNodesInvolvedInPrepare();
    }

    @ManagedAttribute(description = "Number of prepare phases failed due to a TimeoutException since last reset")
    @Metric(displayName = "TimeoutExceptions during prepare")
    public long getTimeoutExceptionOnPrepare(){
        return StatisticsListManager.getTimeoutExceptionsOnPrepare();
    }

    @ManagedAttribute(description = "Number of prepare phases failed due to a DeadlockException since last reset")
    @Metric(displayName = "DeadlockExceptions during prepare")
    public long getDeadlockExceptionOnPrepare(){
        return StatisticsListManager.getDeadlockExceptionsOnPrepare();
    }

    @ManagedAttribute(description = "Cost of a remote get operation ")
    @Metric(displayName = "Remote get operation cost")
    public long getRemoteGetCost(){
        return StatisticsListManager.getRemoteGetCost();
    }

    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset Statistics")
    public void resetStatistics() {
        //StatisticsListManager.clearList();
        StatisticsListManager.reset();
    }

    @ManagedAttribute(description = "Inter-arrival lock request time histogram")
    @Metric(displayName = "Inter-arrival lock request time histogram")
    public Map<Long,Long> getLocksInterArrivalHistogram(){
        return StatisticsListManager.getInterArrivalHistogram();
    }

    @ManagedAttribute(description = "Total execution time of an update transaction (99-th percentile)")
    @Metric(displayName = "Update Tx execution time 99-th percentile")
    public static long getWriteTXDuration99Percentile(){
        return StatisticsListManager.getWriteTXDuration99Percentile();
    }

    @ManagedAttribute(description = "Total execution time of an update transaction (95-th percentile)")
    @Metric(displayName = "Update Tx execution time 95-th percentile")
    public static long getWriteTXDuration95Percentile(){
        return StatisticsListManager.getWriteTXDuration95Percentile();
    }

    @ManagedAttribute(description = "Total execution time of an update transaction (90-th percentile)")
    @Metric(displayName = "Update Tx execution time 90-th percentile")
    public static long getWriteTXDuration90Percentile(){
        return StatisticsListManager.getWriteTXDuration90Percentile();
    }

    @ManagedAttribute(description = "Total execution time of an update transaction (k-th percentile)")
    public static long getWriteTXDurationKPercentile(int k){
        return StatisticsListManager.getWriteTXDurationKPercentile(k);
    }

    @ManagedAttribute(description = "Total execution time of a read-only transaction (99-th percentile)")
    @Metric(displayName = "Update Tx execution time k-th percentile")
    public static long getReadOnlyTXDuration99Percentile(){
        return StatisticsListManager.getReadOnlyTXDuration99Percentile();
    }

    @ManagedAttribute(description = "Total execution time of a read-only transaction (95-th percentile)")
    @Metric(displayName = "Read-only Tx execution time 95-th percentile")
    public static long getReadOnlyTXDuration95Percentile(){
        return StatisticsListManager.getReadOnlyTXDuration95Percentile();
    }

    @ManagedAttribute(description = "Total execution time of a read-only transaction (90-th percentile)")
    @Metric(displayName = "Read-only Tx execution time 90-th percentile")
    public static long getReadOnlyTXDuration90Percentile(){
        return StatisticsListManager.getReadOnlyTXDuration90Percentile();
    }

    @ManagedAttribute(description = "Total execution time of a read-only transaction (k-th percentile)")
    @Metric(displayName = "Read-only Tx execution time k-th percentile")
    public static long getReadOnlyTXDurationKPercentile(int k){
        return StatisticsListManager.getReadOnlyTXDurationKPercentile(k);
    }

    @ManagedAttribute(description = "Number of local rollbacks command")
    @Metric(displayName = "Number of local Rollbacks")
    public static long getLocalRollbacks() {
        return StatisticsListManager.getLocalRollbacks();
    }

    @ManagedAttribute(description = "Number of puts operations in local keys")
    @Metric(displayName = "Number of Put Commands in Local Keys")
    public static long getPutsInLocalKeys() {
        return StatisticsListManager.getLocalPuts();
    }

    @ManagedAttribute(description = "Number of puts operations in remote keys")
    @Metric(displayName = "Number of Put Commands in Remote Keys")
    public static long getPutsInRemoteKeys() {
        return StatisticsListManager.getRemotePuts();
    }

    @ManagedAttribute(description = "Number of gets operations in local keys")
    @Metric(displayName = "Number of Get Commands in Local Keys")
    public static long getGetsInLocalKeys() {
        return StatisticsListManager.getLocalGets();
    }

    @ManagedAttribute(description = "Number of gets operations in remote keys")
    @Metric(displayName = "Number of Get Commands in Remote Keys")
    public static long getGetsInRemoteKeys() {
        return StatisticsListManager.getRemoteGets();
    }
}

