package org.infinispan.stats.percentiles;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 02/05/12
 */
public final class PercentileStatsFactory {

   public static PercentileStats createNewPercentileStats(){
      return new ReservoirSampling();
   }
}
