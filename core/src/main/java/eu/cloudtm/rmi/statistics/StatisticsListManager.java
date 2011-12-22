package eu.cloudtm.rmi.statistics;

import java.lang.Thread.State;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: Davide
 * Date: 20-apr-2011
 * Time: 16.08.54
 * To change this template use File | Settings | File Templates.
 */

public final class StatisticsListManager {

    private static CopyOnWriteArrayList<InfinispanStatistics> statsList=null;

    private StatisticsListManager() {}

    public static synchronized CopyOnWriteArrayList<InfinispanStatistics> getInfinispanStatisticsList()
    {
        if(statsList==null)
            statsList=new CopyOnWriteArrayList<InfinispanStatistics>();

        return statsList;

    }

    public static void addInfinispanStatistics(InfinispanStatistics thread) {
        if(statsList==null)
            statsList=new CopyOnWriteArrayList<InfinispanStatistics>();
        statsList.add(thread);
    }

    public static void removeInfinispanStatistics(InfinispanStatistics thread) {
        statsList.remove(thread);
    }

    public static CopyOnWriteArrayList<InfinispanStatistics> getList(){
        return statsList;
    }
    
    public static synchronized void clearList()
    {
    	Iterator<InfinispanStatistics> iter = statsList.iterator();
    	while(iter.hasNext()){
    		if(iter.next().getThread().getState() == State.TERMINATED)
    			iter.remove();
    	}
    }
}


