package org.infinispan.container.gmu;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/*
* @author Sebastiano Peluso
* @since 5.2
*/
public class FreshnessData {


   private long logicalTimeFreshness;
   private long realTimeFreshness;

   public FreshnessData(long logicalTimeFreshness, long realTimeFreshness){
      this.logicalTimeFreshness = logicalTimeFreshness;
      this.realTimeFreshness = realTimeFreshness;
   }

    public long getLogicalTimeFreshness() {
        return logicalTimeFreshness;
    }

    public long getRealTimeFreshness() {
        return realTimeFreshness;
    }


    public static class Externalizer extends AbstractExternalizer<FreshnessData> {

      @Override
      public Set<Class<? extends FreshnessData>> getTypeClasses() {
         return Util.<Class<? extends FreshnessData>>asSet(FreshnessData.class);
      }

      @Override
      public void writeObject(ObjectOutput output, FreshnessData object) throws IOException {
         output.writeLong(object.logicalTimeFreshness);
         output.writeLong(object.realTimeFreshness);
      }

      @Override
      public FreshnessData readObject(ObjectInput input) throws IOException, ClassNotFoundException {

         long logicalTimeFreshness = input.readLong();
         long realTimeFreshness = input.readLong();

         return new FreshnessData(logicalTimeFreshness, realTimeFreshness);
      }

      @Override
      public Integer getId() {
          return Ids.GMU_FRESHNESS_DATA;
      }
   }
}
