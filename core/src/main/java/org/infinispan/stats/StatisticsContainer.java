package org.infinispan.stats;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 */
public interface StatisticsContainer {

   void addValue(int param, double value);
   long getValue(int param);
   void mergeTo(StatisticsContainer sc);
   int size();
   void dump();

}
