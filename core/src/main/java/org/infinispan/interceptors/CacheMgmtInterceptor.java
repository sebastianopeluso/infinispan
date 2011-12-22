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

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.transaction.Transaction;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Units;

import eu.cloudtm.rmi.statistics.InfinispanStatistics;
import eu.cloudtm.rmi.statistics.StatisticsListManager;
import eu.cloudtm.rmi.statistics.ThreadLocalStatistics;


@MBean(objectName = "TLStatistics", description = "Thread Local statistics interceptor")
public class CacheMgmtInterceptor extends JmxStatsCommandInterceptor {

   private DataContainer dataContainer;

   @Inject
   public void setDependencies(DataContainer dataContainer) {
      this.dataContainer = dataContainer;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
       long t1 = System.nanoTime();
       ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalPrepares();
       //Transaction tx = ctx.getTransaction();
       Transaction tx = ((TxInvocationContext)ctx).getTransaction();
       if (!tx.equals(ThreadLocalStatistics.getInfinispanThreadStats().getLastTransaction())){
               ThreadLocalStatistics.getInfinispanThreadStats().setLastTransaction(tx);
               ThreadLocalStatistics.getInfinispanThreadStats().incrementTransactions();
       }
       Object result = invokeNextInterceptor(ctx, command);
       long t2 = System.nanoTime();
       ThreadLocalStatistics.getInfinispanThreadStats().addTotalPrepareTime(t2-t1);
       return result;
   }

    @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
       long t1 = System.nanoTime();
       ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalCommits();
       Object result = invokeNextInterceptor(ctx, command);
       long t2 = System.nanoTime();
       ThreadLocalStatistics.getInfinispanThreadStats().addTotalCommitTime(t2-t1);
       ThreadLocalStatistics.getInfinispanThreadStats().updateTXStats();
       return result;
   }

    @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      long t1 = System.nanoTime();
      ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalRollbacks();
      Object result = invokeNextInterceptor(ctx, command);
      long t2 = System.nanoTime();
      ThreadLocalStatistics.getInfinispanThreadStats().addTotalRollbackTime(t2-t1);
      ThreadLocalStatistics.getInfinispanThreadStats().updateTXStats();
      return result;
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
       ThreadLocalStatistics.getInfinispanThreadStats().incrementEvictions();
       //ThreadLocalStatistics.getInfinispanThreadStats().setTXWithWrite(false);
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      long t1 = System.nanoTime();
      boolean istx = false;
      Object retval = invokeNextInterceptor(ctx, command);
      if(ctx instanceof TxInvocationContext) {
           //Transaction tx = ((TxInvocationContext)ctx).getTransaction();
           Transaction tx = ((TxInvocationContext)ctx).getTransaction();

           if (!tx.equals(ThreadLocalStatistics.getInfinispanThreadStats().getLastTransaction())){
               ThreadLocalStatistics.getInfinispanThreadStats().setLastTransaction(tx);
               ThreadLocalStatistics.getInfinispanThreadStats().incrementTransactions();
           }
           istx=true;
      }
      long t2 = System.nanoTime();
      if (retval == null) {
         if(istx) {
             ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalMissesTX();
             ThreadLocalStatistics.getInfinispanThreadStats().addMissTotalTXTime(t2-t1);
         }
         else {
             ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalMissesNoTX();
             ThreadLocalStatistics.getInfinispanThreadStats().addMissTotalNoTXTime(t2-t1);
         }
      } else {
         if(istx) {
             ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalHitsTX();
             ThreadLocalStatistics.getInfinispanThreadStats().addHitTotalTXTime(t2-t1);
         }
         else {
             ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalHitsNoTX();
             ThreadLocalStatistics.getInfinispanThreadStats().addHitTotalNoTXTime(t2-t1);
         }
      }
      return retval;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
       long t1 = System.nanoTime();
       boolean istx = false;
       Map data = command.getMap();
       Object retval = invokeNextInterceptor(ctx, command);
       if(ctx instanceof TxInvocationContext) {
           //Transaction tx = ((TxInvocationContext)ctx).getTransaction();
           Transaction tx = ((TxInvocationContext)ctx).getTransaction();
           if (!tx.equals(ThreadLocalStatistics.getInfinispanThreadStats().getLastTransaction())){
               ThreadLocalStatistics.getInfinispanThreadStats().setLastTransaction(tx);
               ThreadLocalStatistics.getInfinispanThreadStats().incrementTransactions();
               ThreadLocalStatistics.getInfinispanThreadStats().setTXReadOnly(false);
           }
           istx=true;
       }
      long t2 = System.nanoTime();
      if (data != null && !data.isEmpty()) {
          if(istx) {
              ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalWritesTX();
              ThreadLocalStatistics.getInfinispanThreadStats().addWritesTXTotalTime(t2-t1);
          }
          else {
              ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalWritesNoTX();
              ThreadLocalStatistics.getInfinispanThreadStats().addWritesNoTXTotalTime(t2-t1);
          }
      }
      return retval;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
       //System.err.println("PUT");
       long t1 = System.nanoTime();
      Object retval = invokeNextInterceptor(ctx, command);
      boolean istx = false;
      if(ctx.isInTxScope()) {
           //Transaction tx = ((TxInvocationContext)ctx).getTransaction();
           Transaction tx = ((TxInvocationContext)ctx).getTransaction();
           if (!tx.equals(ThreadLocalStatistics.getInfinispanThreadStats().getLastTransaction())){
               ThreadLocalStatistics.getInfinispanThreadStats().setLastTransaction(tx);
               ThreadLocalStatistics.getInfinispanThreadStats().incrementTransactions();
               ThreadLocalStatistics.getInfinispanThreadStats().setTXReadOnly(false);
           }
           istx=true;

       }
      long t2 = System.nanoTime();
      if (istx) {
          ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalWritesTX();
          ThreadLocalStatistics.getInfinispanThreadStats().addWritesTXTotalTime(t2-t1);
          //System.out.println("e'una tx");
      }
      else {
          ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalWritesNoTX();
          ThreadLocalStatistics.getInfinispanThreadStats().addWritesNoTXTotalTime(t2-t1);
          //System.out.println("non e'una tx");
      }
      //System.err.println("PUT");
      return retval;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      boolean istx = false;
      if(ctx instanceof TxInvocationContext) {
          //Transaction tx = ((TxInvocationContext)ctx).getTransaction();
          Transaction tx = ((TxInvocationContext)ctx).getTransaction();
          if (!tx.equals(ThreadLocalStatistics.getInfinispanThreadStats().getLastTransaction())){
               ThreadLocalStatistics.getInfinispanThreadStats().setLastTransaction(tx);
               ThreadLocalStatistics.getInfinispanThreadStats().incrementTransactions();
              ThreadLocalStatistics.getInfinispanThreadStats().setTXReadOnly(false);
          }
          istx = true;
      }
      if (retval == null) {
          if(istx)
              ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalRemoveMissesTX();
          else
              ThreadLocalStatistics.getInfinispanThreadStats().incrementTotalRemoveMissesNoTX();
      } else {
          if(istx)
              ThreadLocalStatistics.getInfinispanThreadStats().incrementRemoveHitsTX();
          else
              ThreadLocalStatistics.getInfinispanThreadStats().incrementRemoveHitsNoTX();
      }
      return retval;
   }
   
   @ManagedAttribute(description = "Get Thread Local Statistics")
   @Metric(displayName = "New Infinispan Statistics", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.DETAIL)
   public CopyOnWriteArrayList<InfinispanStatistics> getStatistics() {
      return StatisticsListManager.getList();
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset Statistics")
   public void resetStatistics() {
      StatisticsListManager.clearList();
      System.out.println("ClearList() called");
   }
   
}

