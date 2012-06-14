package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.stats.translations.ExposedStatistics.TransactionalClasses;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public final class TransactionsStatisticsRegistry {

   private static final Log log = LogFactory.getLog(TransactionsStatisticsRegistry.class);

   //Now it is unbounded, we can define a MAX_NO_CLASSES
   private static final Map<TransactionalClasses, NodeScopeStatisticCollector> transactionalClassesStatsMap
         = new EnumMap<TransactionalClasses, NodeScopeStatisticCollector>(TransactionalClasses.class);

   private static final ConcurrentMap<GlobalTransaction, RemoteTransactionStatistics> remoteTransactionStatistics =
         new ConcurrentHashMap<GlobalTransaction, RemoteTransactionStatistics>();

   private static Configuration configuration;

   //Comment for reviewers: do we really need threadLocal? If I have the global id of the transaction, I can
   //retrieve the transactionStatistics
   private static final ThreadLocal<TransactionStatistics> thread = new ThreadLocal<TransactionStatistics>();

   public static void init(Configuration configuration){
      log.tracef("Initializing transactionalClassesMap");
      TransactionsStatisticsRegistry.configuration = configuration;
      transactionalClassesStatsMap.put(TransactionalClasses.DEFAULT_CLASS, new NodeScopeStatisticCollector(configuration));
   }

   public static void addValue(IspnStats param, double value) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to add value " + value + " to parameter " + param +
                         " but no transaction is associated to the thread");
         return;
      }
      txs.addValue(param, value);
   }

   public static void incrementValue(IspnStats param) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to increment to parameter " + param + " but no transaction is associated to the thread");
         return;
      }
      txs.addValue(param, 1D);
   }

   public static void addValueAndFlushIfNeeded(IspnStats param, double value, boolean local) {
      NodeScopeStatisticCollector nssc = transactionalClassesStatsMap.get(TransactionalClasses.DEFAULT_CLASS);
      if (local) {
         nssc.addLocalValue(param, value);
      } else {
         nssc.addRemoteValue(param, value);
      }
   }

   public static void incrementValueAndFlushIfNeeded(IspnStats param, boolean local) {
      NodeScopeStatisticCollector nssc = transactionalClassesStatsMap.get(TransactionalClasses.DEFAULT_CLASS);
      if (local) {
         nssc.addLocalValue(param, 1D);
      } else {
         nssc.addRemoteValue(param, 1D);
      }
   }

   public static void onPrepareCommand() {
      //NB: If I want to give up using the InboundInvocationHandler, I can create the remote transaction
      //here, just overriding the handlePrepareCommand
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to invoke onPrepareCommand() but no transaction is associated to the thread");
         return;
      }
      txs.onPrepareCommand();
   }

   public static void setTransactionOutcome(boolean commit) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to set outcome to " + (commit ? "Commit" : "Rollback") +
                         " but no transaction is associated to the thread");
         return;
      }
      txs.setCommit(commit);
   }

   public static void terminateTransaction() {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to invoke terminate() but no transaction is associated to the thread");
         return;
      }
      txs.terminateTransaction();

      NodeScopeStatisticCollector dest = transactionalClassesStatsMap.get(txs.getTransactionalClass());
      if (dest != null) {
         dest.merge(txs);
      }

      thread.remove();
   }

   public static Object getAttribute(IspnStats param){
      if (configuration == null) {
         return null;
      }
      return transactionalClassesStatsMap.get(TransactionalClasses.DEFAULT_CLASS).getAttribute(param);
   }

   public static Object getPercentile(IspnStats param, int percentile){
      if (configuration == null) {
         return null;
      }
      return transactionalClassesStatsMap.get(TransactionalClasses.DEFAULT_CLASS).getPercentile(param, percentile);
   }

   public static void addTakenLock(Object lock) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to add lock [" + lock + "] but no transaction is associated to the thread");
         return;
      }
      txs.addTakenLock(lock);
   }


   public static void setUpdateTransaction() {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to invoke setUpdateTransaction() but no transaction is associated to the thread");
         return;
      }
      txs.setUpdateTransaction();
   }

   //This is synchronized because depending on the local/remote nature, a different object is created
   //Now, remote transactionStatistics get initialized at InboundInvocationHandler level
   public static void initTransactionIfNecessary(TxInvocationContext tctx) {
      boolean isLocal = tctx.isOriginLocal();
      if(isLocal)
         initLocalTransaction();

   }

   public static void attachRemoteTransactionStatistic(GlobalTransaction globalTransaction, boolean createIfAbsent) {
      RemoteTransactionStatistics rts = remoteTransactionStatistics.get(globalTransaction);
      if (rts == null && createIfAbsent && configuration != null) {
         log.tracef("Create a new remote transaction statistic for transaction %s", globalTransaction);
         rts = new RemoteTransactionStatistics(configuration);
         remoteTransactionStatistics.put(globalTransaction, rts);
      } else if (configuration == null) {
         log.debugf("Trying to create a remote transaction statistics in a not initialized Transaction Statistics Registry");
         return;
      } else {
         log.tracef("Using the remote transaction statistic %s for transaction %s", rts, globalTransaction);
      }
      thread.set(rts);
   }

   public static void detachRemoteTransactionStatistic(GlobalTransaction globalTransaction, boolean finished) {
      if (thread.get() == null) {
         return;
      }
      if (finished) {
         log.tracef("Detach remote transaction statistic and finish transaction %s", globalTransaction);
         terminateTransaction();
         remoteTransactionStatistics.remove(globalTransaction);
      } else {
         log.tracef("Detach remote transaction statistic for transaction %s", globalTransaction);
         thread.remove();
      }
   }

   public static void reset(){
      log.tracef("Reset statistics");
      for (NodeScopeStatisticCollector nsc : transactionalClassesStatsMap.values()) {
         nsc.reset();
      }
   }

   public static boolean hasStatisticCollector() {
      return thread.get() != null;
   }

   private static void initLocalTransaction(){
      //Not overriding the InitialValue method leads me to have "null" at the first invocation of get()
      TransactionStatistics lts = thread.get();
      if (lts == null && configuration != null) {
         log.tracef("Init a new local transaction statistics");
         thread.set(new LocalTransactionStatistics(configuration));
      } else if (configuration == null ) {
         log.debugf("Trying to create a local transaction statistics in a not initialized Transaction Statistics Registry");
      } else {
         log.tracef("Local transaction statistic is already initialized: %s", lts);
      }
   }
}
