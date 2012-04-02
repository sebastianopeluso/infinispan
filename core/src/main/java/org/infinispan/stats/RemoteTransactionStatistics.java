package org.infinispan.stats;

import org.infinispan.stats.translations.ExposedStatistics;
import org.infinispan.stats.translations.RemoteStatistics;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 */
public class RemoteTransactionStatistics extends TransactionStatistics{

   public RemoteTransactionStatistics(){
      super();
      this.statisticsContainer = new StatisticsContainerImpl(RemoteStatistics.NUM_STATS);
   }



   protected int getIndex(ExposedStatistics.IspnStats stat) throws NoIspnStatException{
      int ret = super.getCommonIndex(stat);
      if(ret!=NON_COMMON_STAT)
         return ret;
      switch (stat){
         case REPLAY_TIME:
            return RemoteStatistics.REPLAY_TIME;
         case REPLAYED_TXS:
            return RemoteStatistics.REPLAYED_TXS;
         default:
            throw new NoIspnStatException("Statistic "+stat+" is not available!");
      }

   }




}
