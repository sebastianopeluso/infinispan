package org.infinispan.stats.percentiles;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 */
public interface PercentileStats {

   double getKPercentile(int percentile);
   void insertSample(double value);
   void reset();
}
