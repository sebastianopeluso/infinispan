/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.cloudtm.rmi.statistics;
/**
 *
 * @author Davide
 */
public class ThreadLocalStatistics {

    //private static ThreadStatistics stats = null;


    private static final ThreadLocal<ThreadStatistics> thread = new ThreadLocal<ThreadStatistics>() {
    	
            @Override
            protected ThreadStatistics initialValue() {
                ThreadStatistics stats = new ThreadStatistics(Thread.currentThread());
                StatisticsListManager.addInfinispanStatistics(stats);
                return stats;
            }
    };

    public static ThreadStatistics getInfinispanThreadStats() {
        return thread.get();
    }
    
}
