package org.infinispan.stats;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 */
public class StatisticsContainerImpl implements StatisticsContainer{

   private long[] stats;

   public StatisticsContainerImpl(int size){
      this.stats = new long[size];
   }

   public void addValue(int param, double value){
      this.stats[param]+=value;
   }

   public long getValue(int param){
      return this.stats[param];
   }

   public void mergeTo(StatisticsContainer sc){
      int length = this.stats.length;
      for(int i = 0; i < length; i++){
         sc.addValue(i,this.stats[i]);
      }
   }

   public int size(){
      return this.stats.length;
   }

   public void dump(){
      for(int i=0; i<this.stats.length;i++){
         System.out.println("** "+i+" : "+stats[i]+" **");
      }
   }
}
