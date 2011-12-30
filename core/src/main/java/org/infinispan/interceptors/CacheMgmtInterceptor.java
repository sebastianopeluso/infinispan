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

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader;
import eu.cloudtm.rmi.statistics.StatisticsListManager;
import eu.cloudtm.rmi.statistics.ThreadStatistics;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;

import eu.cloudtm.rmi.statistics.ThreadLocalStatistics;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.Map;


@MBean(objectName = "TLStatistics", description = "Thread Local statistics interceptor")
public class CacheMgmtInterceptor extends JmxStatsCommandInterceptor {

   private Configuration configuration;
   private boolean statisticsEnabled = false;

   private static boolean COMMIT = true;
   private static boolean ABORT = false;


   @Inject
   public void setDependencies(Configuration configuration) {
       this.configuration = configuration;
       this.statisticsEnabled = configuration.isExposeJmxStatistics();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
       if(ctx.isOriginLocal()){
          ThreadStatistics is = ThreadLocalStatistics.getInfinispanThreadStats();
          is.terminateLocalExecution();
       }
       Object result = invokeNextInterceptor(ctx, command);
       return result;
   }

   @Override
   //NB: ORA ANCHE LE RO CHIAMANO IL PREPARE E IL COMMIT COMMAND
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
       long t1 = System.nanoTime();
       Object result = invokeNextInterceptor(ctx, command);
       long t2 = System.nanoTime();
       //DIE
       long commitCost = t2-t1;
       if(ctx.isOriginLocal()){
           ThreadStatistics is = ThreadLocalStatistics.getInfinispanThreadStats();
           is.addCommitCost(commitCost);
           is.flush(COMMIT);
       }
       return result;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      long t1 = System.nanoTime();
      Object result = invokeNextInterceptor(ctx, command);
      long t2 = System.nanoTime();
      //DIE
      if(ctx.isOriginLocal()){
          ThreadStatistics is = ThreadLocalStatistics.getInfinispanThreadStats();
          is.addRollbackCost(t2-t1);
          is.flush(ABORT);
      }

      return result;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
       long t1 = System.nanoTime();
      //DIE
     if(ctx==null){
         throw new RuntimeException("Il contesto è null!!!");
     }

     //DIE
     /*
     La putKeyValueCommand impone al TXInterceptor che venga creata (enlisted) la transazione
     L'enlist fa in modo che io crei la transazione nella threadLocal. Quindi se accedo alla tx del threadLocal
     da qui, creo la nullPointerException. Questo va sul tx Interceptor, dopo la enlist
      */

     /*
      if(ctx.isInTxScope()){
         StatisticsListManager.insertInterArrivalSample((long)(t1/1000.0D), command.getKey());
          if(ctx.isOriginLocal()){
            System.out.println("Put di una tx locale");
            ThreadLocalStatistics.getInfinispanThreadStats().incrementPuts();
          }
      }
      */
      return invokeNextInterceptor(ctx, command);
   }


   /*
   JMX-EXPOSED METHODS
   */

   @ManagedAttribute(description = "Average Rtt duration")
   @Metric(displayName = "Rtt")
   public long getRollbacks() {
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

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset Statistics")
   public void resetStatistics() {
      StatisticsListManager.clearList();
   }

   @ManagedAttribute(description = "Inter-arrival lock request time histogram")
   @Metric(displayName = "Inter-arrival lock request time histogram")
   public Map<Long,Long> getLocksInterArrivalHistogram(){
       return StatisticsListManager.getInterArrivalHistogram();
   }


   @ManagedAttribute(description = "Test")
   @Metric(displayName = "Test")
   public long getTest(){
       return 12L;
   }


}

