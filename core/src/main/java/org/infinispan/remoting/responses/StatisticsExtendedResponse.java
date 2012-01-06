package org.infinispan.remoting.responses;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: diego
 * Date: 23/12/11
 * Time: 18:39
 * To change this template use File | Settings | File Templates.
 */
public class StatisticsExtendedResponse extends ExtendedResponse {

   private long replayTime = 0;

   public StatisticsExtendedResponse(Response response, boolean replayIgnoredRequests, long replayTime){
       super(response,replayIgnoredRequests);
       this.replayTime = replayTime;
   }

   public long getReplayTime(){
      return this.replayTime;
   }

   public void setReplayTime(long rt){
       this.replayTime=rt;
   }


    public static class Externalizer extends AbstractExternalizer<StatisticsExtendedResponse> {
      @Override
      public void writeObject(ObjectOutput output, StatisticsExtendedResponse ser) throws IOException {
         output.writeBoolean(ser.replayIgnoredRequests);
         output.writeObject(ser.response);
         output.writeLong(ser.replayTime);
      }

      @Override
      public StatisticsExtendedResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         boolean replayIgnoredRequests = input.readBoolean();
         Response response = (Response) input.readObject();
         long timeToReplay =  input.readLong();
         return new StatisticsExtendedResponse(response, replayIgnoredRequests,timeToReplay);
      }

      @Override
      public Integer getId() {
         return Ids.STATISTICS_EXTENDED_RESPONSE;
      }

      @Override
      public Set<Class<? extends StatisticsExtendedResponse>> getTypeClasses() {
         return Util.<Class<? extends StatisticsExtendedResponse>>asSet(StatisticsExtendedResponse.class);
      }
   }
}
