package org.infinispan.stats;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.stats.translations.LocalRemoteStatistics;
import org.infinispan.stats.translations.LocalStatistics;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 */
public class LocalTransactionStatistics extends TransactionStatistics {



   private boolean stillLocalExecution;

   public LocalTransactionStatistics(){
      super();
      this.stillLocalExecution = true;
      this.statisticsContainer = new StatisticsContainerImpl(LocalStatistics.NUM_STATS);
   }



   public void terminateLocalExecution(){
      this.stillLocalExecution = false;
      this.addValue(IspnStats.WR_TX_LOCAL_EXECUTION_TIME,System.nanoTime() - this.initTime);
      this.incrementValue(IspnStats.NUM_PREPARES);
   }

   public boolean isStillLocalExecution(){
      return this.stillLocalExecution;
   }

   protected int getIndex(IspnStats stat) throws NoIspnStatException{
      int ret = super.getCommonIndex(stat);
      if(ret!=NON_COMMON_STAT)
         return ret;

      switch (stat){
         case LOCAL_CONTENTION_PROBABILITY:
            return LocalStatistics.LOCAL_CONTENTION_PROBABILITY;
         case WR_TX_LOCAL_EXECUTION_TIME:
            return LocalStatistics.WR_TX_LOCAL_EXECUTION_TIME;
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            return LocalStatistics.WR_TX_SUCCESSFUL_LOCAL_EXECUTION_TIME;
         case PUTS_PER_LOCAL_TX:
            return LocalStatistics.PUTS_PER_LOCAL_TX;
         case PREPARE_COMMAND_SIZE:
            return LocalStatistics.PREPARE_COMMAND_SIZE;
         case NUM_NODES_IN_PREPARE:
            return LocalStatistics.NUM_NODES_IN_PREPARE;
         case NUM_PREPARES:
            return LocalStatistics.NUM_PREPARE;
         case RTT:
            return LocalStatistics.RTT;
         case NUM_SUCCESSFUL_RTTS:
            return LocalStatistics.NUM_RTTS;


         default:
            throw new NoIspnStatException("IspnStats "+stat+" not found!");
      }
   }






}
