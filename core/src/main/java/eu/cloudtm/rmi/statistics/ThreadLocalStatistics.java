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
    private static InfinispanStatistics stats = null;
    private static final ThreadLocal<InfinispanStatistics> thread = new ThreadLocal<InfinispanStatistics>() {
    	
            @Override
            protected InfinispanStatistics initialValue() {
                stats = new InfinispanStatistics(Thread.currentThread());
                StatisticsListManager.addInfinispanStatistics(stats);
                return stats;
            }
    };

    public static InfinispanStatistics getInfinispanThreadStats() {
        return thread.get();
    }
    
}
