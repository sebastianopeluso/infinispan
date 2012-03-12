package org.infinispan.executors;

/**
 * Expose some methods to control this executor service as a {@link java.util.concurrent.ThreadPoolExecutor}
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ControllableExecutorService {

   /**
    * returns the core number of threads
    * @return the core number of threads
    */
   int getCorePoolSize();

   /**
    * returns the maximum allowed number of threads
    * @return the maximum allowed number of threads                                       
    */
   int getMaximumPoolSize();

   /**
    * returns the thread keep-alive time, which is the amount of time which threads in excess of the core pool size 
    * may remain idle before being terminated
    * @return the time limit in milliseconds
    */
   long getKeepAliveTime();

   /**
    * returns the occupation percentage of the queue, i.e., the number of pending task over the capacity of the qeue
    * @return the occupation percentage of the queue
    */
   double getQueueOccupationPercentage();

   /**
    * returns the usage percentage of this executor service, i.e, the number of threads running tasks over the maximum
    * number of threads that the executor service can create
    * @return the usage percentage
    */
   double getUsagePercentage();

   /**
    * Sets the core number of threads. This overrides any value set in the constructor. 
    * If the new value is smaller than the current value, excess existing threads will be terminated when they next become idle. 
    * If larger, new threads will, if needed, be started to execute any queued tasks.  
    * @param size the new core size
    */
   void setCorePoolSize(int size);

   /**
    * Sets the maximum allowed number of threads. This overrides any value set in the constructor. 
    * If the new value is smaller than the current value, excess existing threads will be terminated when they next become idle. 
    * @param size the new maximum 
    */
   void setMaximumPoolSize(int size);

   /**
    * Sets the time limit for which threads may remain idle before being terminated. 
    * If there are more than the core number of threads currently in the pool, after waiting this amount of time 
    * without processing a task, excess threads will be terminated. This overrides any value set in the constructor. 
    * @param milliseconds the time to wait in milliseconds. A time value of zero will cause excess threads to terminate immediately after executing tasks.
    */
   void setKeepAliveTime(long milliseconds);

}
