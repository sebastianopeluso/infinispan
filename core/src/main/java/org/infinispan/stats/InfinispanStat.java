package org.infinispan.stats;

import org.infinispan.stats.translations.ExposedStatistics;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 */
public interface InfinispanStat {



   long getValue(ExposedStatistics.IspnStats param);
   void addValue(ExposedStatistics.IspnStats param, double delta);
   void incrementValue(ExposedStatistics.IspnStats param);

}
