package org.infinispan.stats;

import java.util.HashMap;
import java.util.logging.Logger;

import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.stats.translations.ExposedStatistics;
import org.infinispan.stats.translations.LocalRemoteStatistics;


/**
 * Created by IntelliJ IDEA.
 * User: Davide
 * Date: 20-apr-2011
 * Time: 16.48.47
 * To change this template use File | Settings | File Templates.
 */
public abstract class TransactionStatistics implements InfinispanStat {


   /*
   Here the elements which are common for local and remote transactions
    */
   protected long initTime;
   private boolean isReadOnly;
   private boolean isCommit;
   private ExposedStatistics.TransactionalClasses transactionalClass;
   private HashMap<Object, Long> takenLocks = new HashMap<Object, Long>();
   protected final static int NON_COMMON_STAT = -1;


   protected StatisticsContainer statisticsContainer;

   private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TransactionStatistics.class);


   public TransactionStatistics() {
      this.initTime = System.nanoTime();
      this.isReadOnly = true; //as far as it does not tries to perform a put operation
      this.takenLocks = new HashMap<Object, Long>();
      this.transactionalClass = ExposedStatistics.TransactionalClasses.DEFAULT_CLASS;
      log.warn("TRANSACTION_STATISTIC CREATED");
   }

   public ExposedStatistics.TransactionalClasses getTransactionalClass(){
      return this.transactionalClass;
   }

   public boolean isCommit(){
      return this.isCommit;
   }

   public boolean isReadOnly(){
      return this.isReadOnly;
   }

   public void setUpdateTransaction(){
      this.isReadOnly = false;
   }

   public final void addTakenLock(Object lock){
      if(!this.takenLocks.containsKey(lock))
         this.takenLocks.put(lock, System.nanoTime());
   }

   protected abstract int getIndex(IspnStats param);

   public void addValue(IspnStats param, double value){
      int index = this.getIndex(param);
      this.statisticsContainer.addValue(index,value);
   }

   public long getValue(IspnStats param){
      int index = this.getIndex(param);
      return this.statisticsContainer.getValue(index);
   }

   public void incrementValue(IspnStats param){
      this.addValue(param,1);
   }



   public void terminateTransaction(boolean commit) {



      this.isCommit = commit;

      long now = System.nanoTime();
      double execTime = now - this.initTime;
      if(this.isReadOnly){
         if(commit){
            this.incrementValue(IspnStats.NUM_COMMITTED_RO_TX);
            this.addValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME,execTime);
         }
         else{
            this.incrementValue(IspnStats.NUM_ABORTED_RO_TX);
            this.addValue(IspnStats.RO_TX_ABORTED_EXECUTION_TIME,execTime);
         }
      }
      else{
         if(commit){
            this.incrementValue(IspnStats.NUM_COMMITTED_WR_TX);
            this.addValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME,execTime);
            long numPuts = this.getValue(IspnStats.NUM_PUTS);
            this.addValue(IspnStats.NUM_SUCCESSFUL_PUTS,numPuts);
         }
         else{
            this.incrementValue(IspnStats.NUM_ABORTED_WR_TX);
            this.addValue(IspnStats.WR_TX_ABORTED_EXECUTION_TIME,execTime);
         }
      }


      int heldLocks = this.takenLocks.size();
      double cumulativeLockHoldTime = this.computeCumulativeLockHoldTime(heldLocks,now);
      this.addValue(IspnStats.NUM_HELD_LOCKS,heldLocks);
      this.addValue(IspnStats.LOCK_HOLD_TIME,cumulativeLockHoldTime);

      this.dump();

   }

   private long computeCumulativeLockHoldTime(int numLocks,long currentTime){
      long ret = numLocks * currentTime;
      for(Object o:this.takenLocks.keySet())
         ret-=this.takenLocks.get(o);
      return ret;
   }

   public void flush(TransactionStatistics ts){
      this.statisticsContainer.mergeTo(ts.statisticsContainer);
   }


   protected int getCommonIndex(IspnStats stat){
      switch (stat){
         case LOCK_HOLD_TIME:
            return LocalRemoteStatistics.LOCK_HOLD_TIME;
         case  NUM_HELD_LOCKS:
            return LocalRemoteStatistics.NUM_HELD_LOCK;
         case LOCK_WAITING_TIME:
            return LocalRemoteStatistics.LOCK_WAITING_TIME;
         case ROLLBACK_EXECUTION_TIME:
            return LocalRemoteStatistics.ROLLBACK_EXECUTION_TIME;
         case COMMIT_EXECUTION_TIME:
            return LocalRemoteStatistics.COMMIT_EXECUTION_TIME;
         case NUM_COMMIT_COMMAND:
            return LocalRemoteStatistics.NUM_COMMIT_COMMAND;
         case NUM_ROLLBACKS:
            return LocalRemoteStatistics.NUM_ROLLBACKS;
         case NUM_PUTS:
            return LocalRemoteStatistics.NUM_PUTS;
         case NUM_SUCCESSFUL_RTTS:
            return LocalRemoteStatistics.NUM_SUCCESSFUL_RTTS;
         case NUM_WAITED_FOR_LOCKS:
            return LocalRemoteStatistics.NUM_WAITED_FOR_LOCKS;
         case NUM_COMMITTED_RO_TX:
            return LocalRemoteStatistics.NUM_COMMITTED_RO_TX;
         case NUM_ABORTED_RO_TX:
            return LocalRemoteStatistics.NUM_ABORTED_RO_TX;
         case NUM_ABORTED_WR_TX:
            return LocalRemoteStatistics.NUM_ABORTED_WR_TX;
         case NUM_COMMITTED_WR_TX:
            return LocalRemoteStatistics.NUM_COMMITTED_WR_TX;
         case LOCK_CONTENTION_TO_LOCAL:
            return LocalRemoteStatistics.LOCK_CONTENTION_TO_LOCAL;
         case LOCK_CONTENTION_TO_REMOTE:
            return LocalRemoteStatistics.LOCK_CONTENTION_TO_REMOTE;
         case NUM_REMOTE_GET:
            return LocalRemoteStatistics.NUM_REMOTE_GET;
         case NUM_REMOTE_PUT:
            return LocalRemoteStatistics.NUM_REMOTE_PUT;
         case REMOTE_GET_EXECUTION:
            return LocalRemoteStatistics.REMOTE_GET_EXECUTION;
         case REMOTE_PUT_EXECUTION:
            return LocalRemoteStatistics.REMOTE_PUT_EXECUTION;
         case WR_TX_ABORTED_EXECUTION_TIME:
            return LocalRemoteStatistics.WR_TX_ABORTED_EXECUTION_TIME;
         case RO_TX_ABORTED_EXECUTION_TIME:
            return LocalRemoteStatistics.RO_TX_ABORTED_EXECUTION_TIME;
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            return LocalRemoteStatistics.WR_TX_SUCCESSFUL_EXECUTION_TIME;
         case RO_TX_SUCCESSFUL_EXECUTION_TIME:
            return LocalRemoteStatistics.RO_TX_SUCCESSFUL_EXECUTION_TIME;
      }
      return NON_COMMON_STAT;

   }



   public void dump(){
      this.statisticsContainer.dump();
   }





}

