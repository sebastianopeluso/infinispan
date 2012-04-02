package org.infinispan.stats.percentiles;



import org.infinispan.stats.percentiles.PercentileStats;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: roberto
 * Date: 01/01/12
 * Time: 18:38
 * To change this template use File | Settings | File Templates.
 */




public class ReservoirSampling implements PercentileStats {

    private static final int DEFAULT_NUM_SPOTS = 100;
    private long SEED = System.nanoTime();
    private double[] reservoir;
    private AtomicInteger index;
    private int NUM_SPOT;
    Random rand;

    public ReservoirSampling(){
        NUM_SPOT = DEFAULT_NUM_SPOTS;
        this.reservoir = new double[NUM_SPOT];
        this.index = new AtomicInteger(0);
        rand = new Random(SEED);
    }

    public ReservoirSampling(int numSpots){
        this.NUM_SPOT = numSpots;
        this.reservoir = new double[NUM_SPOT];
        this.index = new AtomicInteger(0);
        rand = new Random(SEED);

    }

    public  void insertSample(double sample){
        int i = index.getAndIncrement();
        if(i < NUM_SPOT)
            reservoir[i]=sample;
        else{
            int rand_generated = rand.nextInt(i+2);//should be nextInt(index+1) but nextInt is exclusive
            if(rand_generated < NUM_SPOT){
                reservoir[rand_generated]=sample;
            }
        }
    }

    public long get95Percentile(){
        int[] copy = new int[NUM_SPOT];
        System.arraycopy(this.reservoir,0,copy,0,NUM_SPOT);
        Arrays.sort(copy);
        return copy[this.getIndex(95)];
    }

    public long get90Percentile(){
       int[] copy = new int[NUM_SPOT];
        System.arraycopy(this.reservoir,0,copy,0,NUM_SPOT);
        Arrays.sort(copy);
        return copy[this.getIndex(90)];
    }

    public long get99Percentile(){
        int[] copy = new int[NUM_SPOT];
        System.arraycopy(this.reservoir,0,copy,0,NUM_SPOT);
        Arrays.sort(copy);
        return copy[this.getIndex(99)];
    }

    public double getKPercentile(int k){
        if(k<0 || k>100)
            throw new RuntimeException("Wrong index in getKpercentile");
        int[] copy = new int[NUM_SPOT];
        System.arraycopy(this.reservoir,0,copy,0,NUM_SPOT);
        Arrays.sort(copy);
        return copy[this.getIndex(k)];
    }

    private int getIndex(int k){
        //I solve the proportion k:100=x:NUM_SAMPLE
        //Every percentage is covered by NUM_SAMPLE / 100 buckets; I consider here only the first as representative
        //of a percentage
        return (int) (NUM_SPOT * (k-1) / 100);
    }

    public void reset(){
        this.index.set(0);
        this.reservoir = new double[NUM_SPOT];
    }




}


