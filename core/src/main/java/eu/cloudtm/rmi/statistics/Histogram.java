package eu.cloudtm.rmi.statistics;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: Diego
 * Date: 22/05/11
 * Time: 14:56
 * To change this template use File | Settings | File Templates.
 */

public class Histogram {

    private AtomicInteger[] buckets;
    private int step;
    private long min;
    private long max;
    private int numBuckets;


    private String fileName;

    public Histogram(long min,long max, int step){
        this.step=step;
        this.min=min;
        this.max=max;
        this.numBuckets=(int)(max-min)/step;
        this.buckets= new AtomicInteger[numBuckets];
        for(int i=0;i<numBuckets;i++){
            this.buckets[i]=new AtomicInteger(0);
        }
        this.fileName  = "Histogram.txt";

    }


    public Histogram(long min,long max, int step, String name){
        this.step=step;
        this.min=min;
        this.max=max;
        this.numBuckets=(int)(max-min)/step;
        this.buckets= new AtomicInteger[numBuckets];
        for(int i=0;i<numBuckets;i++){
            this.buckets[i]=new AtomicInteger(0);
        }
        this.fileName  = name;

    }

    public void insertSample(double sample){
        int index=determineIndex(sample);
        this.buckets[index].incrementAndGet();
    }

    private int determineIndex(double sample){
        int index;
        if(sample>max){
            index=numBuckets-1;
        }
        else if(sample<=min){
            index=0;
        }
        else{
            index =(int) ((Math.ceil((sample-min)/step))-1);
        }
        return index;
    }

    public void dumpHistogram(){
        try{
            File f= new File(this.fileName);
            PrintWriter pw= new PrintWriter(f);
            for(int k=0;k<this.numBuckets;k++){
                pw.println(step*(k+1)+"\t"+this.buckets[k].get());
            }
            pw.close();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    public SortedMap<Long,Long> getLockInterArrivalRates(){
        SortedMap<Long, Long> map = new TreeMap<Long, Long>();
        for(int i=0;i<this.buckets.length;i++){
            map.put(new Long(step*(i+1)),new Long(this.buckets[i].get()));
        }

        return map;

    }





}
