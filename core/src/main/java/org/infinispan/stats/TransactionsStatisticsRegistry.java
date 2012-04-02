package org.infinispan.stats;

import org.apache.log4j.Logger;
import org.infinispan.context.impl.TxInvocationContext;
import java.util.HashMap;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.stats.translations.ExposedStatistics;
import org.w3c.dom.Node;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 */
public class TransactionsStatisticsRegistry {

   private static Logger log = Logger.getLogger(TransactionsStatisticsRegistry.class);

   //Now it is unbounded, we can define a MAX_NO_CLASSES
   private static HashMap<ExposedStatistics.TransactionalClasses, NodeScopeStatisticCollector> transactionalClassesStatsMap = new HashMap<ExposedStatistics.TransactionalClasses, NodeScopeStatisticCollector>();
   public static int LOCAL_PARAM = 0;
   public static int REMOTE_PARAM = 1;
   public static int GLOBAL_PARAM = 2;

   public static void init(){

      transactionalClassesStatsMap.put(ExposedStatistics.TransactionalClasses.DEFAULT_CLASS, new NodeScopeStatisticCollector());
      log.debug("Initializing transactionalClassesMap");
   }

   /*
  Comment for reviewers: do we really need threadLocal? If I have the global id of the transaction, I can
  retrieve the transactionStatistics
   */
   private static final ThreadLocal<TransactionStatistics> thread = new ThreadLocal<TransactionStatistics>();


   public static void addValue(IspnStats param, double value) {
      TransactionStatistics txs = thread.get();
      txs.addValue(param, value);
   }

   public static void incrementValue(IspnStats param) {
      TransactionStatistics txs = thread.get();
      txs.addValue(param, 1D);
   }

   public static void terminateLocalExecution() {
      LocalTransactionStatistics ltxs = (LocalTransactionStatistics) thread.get();
      ltxs.terminateLocalExecution();
   }

   public static void terminateTransaction(boolean commit, TxInvocationContext tctx) {

      log.fatal("TERMINATING_TRANSACTIOn");
      TransactionStatistics txs = thread.get();
      txs.terminateTransaction(commit);

      NodeScopeStatisticCollector dest = transactionalClassesStatsMap.get(txs.getTransactionalClass());
      dest.merge(txs);

      thread.remove();

   }

   public static Object getAttribute(ExposedStatistics.IspnStats param){
      log.warn("Going to invoke getAttrbibute with parameter "+param);
      return transactionalClassesStatsMap.get(ExposedStatistics.TransactionalClasses.DEFAULT_CLASS).getAttribute(param);
   }

   public static void addTakenLock(Object lock) {
      TransactionStatistics txs = thread.get();
      txs.addTakenLock(lock);
   }


   public static void setUpdateTransaction() {
      TransactionStatistics txs = thread.get();
      txs.setUpdateTransaction();
   }

   //This is synchronized because depending on the local/remote nature, a different object is created
   //Now, remote transactionStatistics get initialized at InboundInvocationHandler level
   public static void initTransactionIfNecessary(TxInvocationContext tctx) {
      boolean isLocal = tctx.isOriginLocal();
      if(isLocal)
         initTransaction(isLocal);

   }

   public static void initRemoteTransaction(){
      initTransaction(false);
   }


   private static void initTransaction(boolean isLocal){
       //Not overriding the InitialValue method leads me to have "null" at the first invocation of get()
      if (thread.get() == null) {
         log.fatal("THREAD.GET==NULL!!!!");
         if (isLocal) {
            thread.set(new LocalTransactionStatistics());
         } else {
            thread.set(new RemoteTransactionStatistics());
         }
      }
      else
         log.fatal("THREAD.GET!=NULL!!!!");
   }

   public static void reset(){
      for(ExposedStatistics.TransactionalClasses n: transactionalClassesStatsMap.keySet()){
         transactionalClassesStatsMap.get(n).reset();
      }
   }


}
