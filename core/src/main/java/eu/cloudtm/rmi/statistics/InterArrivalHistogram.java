package eu.cloudtm.rmi.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by IntelliJ IDEA.
 * User: Diego
 * Date: 22/05/11
 * Time: 17:41
 * To change this template use File | Settings | File Templates.
 */
public class InterArrivalHistogram {


    private Histogram histo;
    private ConcurrentHashMap<Object,AtomicLong> lastSeen= new ConcurrentHashMap<Object,AtomicLong>(100000);

    public InterArrivalHistogram(long min, long max, int step){
        this.histo=new Histogram(min,max,step);
    }

    public void insertSample(long time, Object key){

        /*
        Not really thread safe, but just for the first time. Ci andrebbe un testAndSet
         */

        if(!lastSeen.containsKey(key)){
            lastSeen.put(key,new AtomicLong(time));
        }
        else{
            long pre = lastSeen.get(key).getAndSet(time);
            double sample= time-pre;
            histo.insertSample(sample);
        }
    }

    public void dumpHistogram(){
        this.histo.dumpHistogram();
    }

    public Map<Long,Long> getInterArrivalHistogram(){
        return histo.getLockInterArrivalRates();
    }


}
