package org.infinispan.stats.topK;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 12/20/11
 * Time: 6:23 PM
 *
 * @author pruivo
 */
public class AnalyticsBean {

    private static final AnalyticsBean instance = new AnalyticsBean();
    public static final int MAX_CAPACITY = 100;


    //counters
    private StreamSummary<Object> remoteGet;
    private StreamSummary<Object> localGet;
    private StreamSummary<Object> remotePut;
    private StreamSummary<Object> localPut;

    private StreamSummary<Object> mostLockedKey;
    private StreamSummary<Object> mostContendedKey; //keys with more contention detected
    private StreamSummary<Object> mostFailedKey;

    private int capacity = 100;
    private boolean active = false;


    public static enum Stat {
        REMOTE_GET,
        LOCAL_GET,
        REMOTE_PUT,
        LOCAL_PUT,

        MOST_LOCKED_KEYS,
        MOST_CONTENDED_KEYS,
        MOST_FAILED_KEYS
    }




    private AnalyticsBean() {
        remoteGet = new StreamSummary<Object>(MAX_CAPACITY);
        localGet = new StreamSummary<Object>(MAX_CAPACITY);
        remotePut = new StreamSummary<Object>(MAX_CAPACITY);
        localPut = new StreamSummary<Object>(MAX_CAPACITY);
        mostLockedKey = new StreamSummary<Object>(MAX_CAPACITY);
        mostContendedKey = new StreamSummary<Object>(MAX_CAPACITY);
        mostFailedKey = new StreamSummary<Object>(MAX_CAPACITY);
    }

    public static AnalyticsBean getInstance() {
        return instance;
    }

    public int getCapacity() {
        return capacity;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public void setCapacity(int capacity) {
        if(capacity <= 0) {
            this.capacity = 1;
        } else {
            this.capacity = capacity;
        }
    }

    public void addGet(Object key, boolean remote) {
        if(!isActive()) {
            return;
        }
        if(remote) {
            synchronized (remoteGet) {
                remoteGet.offer(key);
            }
        } else {
            synchronized (localGet) {
                localGet.offer(key);
            }
        }
    }

    public void addPut(Object key, boolean remote) {
        if(!isActive()) {
            return;
        }
        if(remote) {
            synchronized (remotePut) {
                remotePut.offer(key);
            }
        } else {
            synchronized (localPut) {
                localPut.offer(key);
            }
        }
    }

    public void addLockInformation(Object key, boolean contention, boolean abort) {
        if(!isActive()) {
            return;
        }
        synchronized (mostLockedKey) {
            mostLockedKey.offer(key);
        }
        if(contention) {
            synchronized (mostContendedKey) {
                mostContendedKey.offer(key);
            }
        }
        if(abort) {
            synchronized (mostFailedKey) {
                mostFailedKey.offer(key);
            }
        }
    }

    public Map<Object, Long> getTopKFrom(Stat stat) {
        return getTopKFrom(stat, capacity);
    }

    public Map<Object, Long> getTopKFrom(Stat stat, int topk) {
        switch (stat) {
            case REMOTE_GET: return getStatsFrom(remoteGet, topk);
            case LOCAL_GET: return getStatsFrom(localGet, topk);
            case REMOTE_PUT: return getStatsFrom(remotePut, topk);
            case LOCAL_PUT: return getStatsFrom(localPut, topk);
            case MOST_LOCKED_KEYS: return getStatsFrom(mostLockedKey, topk);
            case MOST_FAILED_KEYS: return getStatsFrom(mostFailedKey, topk);
            case MOST_CONTENDED_KEYS: return getStatsFrom(mostContendedKey, topk);
        }
        return Collections.emptyMap();
    }

    private Map<Object, Long> getStatsFrom(StreamSummary<Object> ss, int topk) {
        List<Counter<Object>> counters = ss.topK(topk <= 0 ? 1 : topk);
        Map<Object, Long> results = new HashMap<Object, Long>(topk);

        for(Counter<Object> c : counters) {
            results.put(c.getItem(), c.getCount());
        }

        return results;
    }

    public void resetAll(){
        synchronized (remoteGet){
            remoteGet = new StreamSummary<Object>(MAX_CAPACITY);
        }
        synchronized (remotePut){
            remotePut = new StreamSummary<Object>(MAX_CAPACITY);
        }
        synchronized (localPut){
            localPut = new StreamSummary<Object>(MAX_CAPACITY);
        }
        synchronized (localGet){
            localGet = new StreamSummary<Object>(MAX_CAPACITY);
        }
        synchronized (mostContendedKey){
            mostContendedKey = new StreamSummary<Object>(MAX_CAPACITY);
        }
        synchronized (mostFailedKey){
            mostFailedKey = new StreamSummary<Object>(MAX_CAPACITY);
        }
        synchronized (mostLockedKey){
            mostLockedKey = new StreamSummary<Object>(MAX_CAPACITY);
        }

    }

    public void resetStat(Stat stat){
        switch (stat){
            case REMOTE_GET: remoteGet = new StreamSummary<Object>(MAX_CAPACITY);
            case LOCAL_GET: localGet = new StreamSummary<Object>(MAX_CAPACITY);
            case REMOTE_PUT: remotePut = new StreamSummary<Object>(MAX_CAPACITY);
            case LOCAL_PUT: localPut = new StreamSummary<Object>(MAX_CAPACITY);
            case MOST_LOCKED_KEYS: mostLockedKey = new StreamSummary<Object>(MAX_CAPACITY);
            case MOST_FAILED_KEYS: mostFailedKey = new StreamSummary<Object>(MAX_CAPACITY);
            case MOST_CONTENDED_KEYS: mostContendedKey = new StreamSummary<Object>(MAX_CAPACITY);
        }
    }
}
