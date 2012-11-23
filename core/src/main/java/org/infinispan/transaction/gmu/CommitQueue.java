package org.infinispan.transaction.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUEntryVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class CommitQueue {

   private final static Log log = LogFactory.getLog(CommitQueue.class);

   private final CurrentVersion currentVersion;
   private final TransactionQueue transactionQueue;
   private final CommitThread commitThread;
   private CommitInstance commitInvocationInstance;
   private InterceptorChain ic;
   private InvocationContextContainer icc;
   private GMUVersionGenerator versionGenerator;
   private CommitLog commitLog;

   public CommitQueue() {
      commitThread = new CommitThread();
      transactionQueue = new TransactionQueue();
      currentVersion = new CurrentVersion();
   }

   @Inject
   public void inject(InterceptorChain ic, InvocationContextContainer icc, VersionGenerator versionGenerator,
                      CommitLog commitLog) {
      this.ic = ic;
      this.icc = icc;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      this.commitLog = commitLog;
   }

   //AFTER THE VersionGenerator
   @Start(priority = 31)
   public void start() {
      commitThread.start();
      currentVersion.init();

      if(commitInvocationInstance == null) {
         List<CommandInterceptor> all = ic.getInterceptorsWhichExtend(EntryWrappingInterceptor.class);
         if(log.isDebugEnabled()) {
            log.debugf("Starting Commit Queue Component. Searching interceptors with interface CommitInstance. " +
                             "Found: %s", all);
         }
         for(CommandInterceptor ci : all) {
            if(ci instanceof CommitInstance) {
               if(log.isDebugEnabled()) {
                  log.debugf("Interceptor implementing CommitInstance found! It is %s", ci);
               }
               commitInvocationInstance = (CommitInstance) ci;
               break;
            }
         }
      }
      if(commitInvocationInstance == null) {
         throw new NullPointerException("Commit Invocation Instance must not be null in serializable mode.");
      }
   }

   @Stop
   public void stop() {
      commitThread.interrupt();
      //TODO
   }

   /**
    * add a transaction to the queue. A temporary commit vector clock is associated
    * and with it, it order the transactions. this commit vector clocks is returned.
    * @param cacheTransaction the transaction to be prepared    
    * @return the prepare vector clock
    */
   public void prepareTransaction(CacheTransaction cacheTransaction) {
      transactionQueue.addTransaction(cacheTransaction);
   }

   public void rollbackTransaction(CacheTransaction cacheTransaction) {
      transactionQueue.remove(cacheTransaction.getGlobalTransaction());
   }

   public void commitTransaction(CacheTransaction cacheTransaction) {
      transactionQueue.updateTransaction(cacheTransaction);
   }

   public void waitUntilCommitted(CacheTransaction cacheTransaction) throws InterruptedException {
      TransactionEntry entry = transactionQueue.getTransactionEntry(cacheTransaction.getGlobalTransaction());
      if (entry != null) {
         entry.waitUntilCommitted();
      }
   }

   public void prepareReadOnlyTransaction(CacheTransaction cacheTransaction) {
      EntryVersion preparedVersion = currentVersion.getNextPrepareVersion(cacheTransaction.getTransactionVersion());
      cacheTransaction.setTransactionVersion(preparedVersion);
   }

   private static class TransactionEntry implements Comparable<TransactionEntry> {
      private int index;
      private final CacheTransaction cacheTransaction;
      private GMUEntryVersion entryVersion;
      private boolean ready;
      private boolean committed;
      private long headStartTs;

      private TransactionEntry(CacheTransaction cacheTransaction) {
         this.cacheTransaction = cacheTransaction;
         this.entryVersion = toGMUEntryVersion(cacheTransaction.getTransactionVersion());
         this.index = 0;
         this.ready = false;
         this.committed = false;
         this.headStartTs = 0;
      }

      public synchronized void setIndex(int index) {
         this.index = index;
         if (index == 0) {
            setHeadStartTs();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Updated transaction index in queue: %s", this);
         }
      }

      public synchronized void incrementIndex() {
         index++;
         if (log.isTraceEnabled()) {
            log.tracef("Increment transaction index in queue: %s", this);
         }
      }

      public synchronized void decrementIndex(int decrement) {
         index -= decrement;
         if (index == 0) {
            setHeadStartTs();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Decrement transaction index in queue: %s", this);
         }
      }

      public synchronized int getIndex() {
         return index;
      }

      public synchronized void commitVersion(GMUEntryVersion entryVersion) {
         this.entryVersion = entryVersion;
         ready = true;
         if (log.isTraceEnabled()) {
            log.tracef("Set transaction commit version: %s", this);
         }
      }

      public synchronized GMUEntryVersion getVersion() {
         return entryVersion;
      }

      public CacheTransaction getCacheTransaction() {
         return cacheTransaction;
      }

      public synchronized boolean isReady() {
         return ready;
      }

      public GlobalTransaction getGlobalTransaction() {
         return cacheTransaction.getGlobalTransaction();
      }

      public void setHeadStartTs() {
         this.headStartTs = System.nanoTime();
      }

      /*public long getHeadStartTs() {
         return headStartTs;
      }*/

      public synchronized void committed() {
         committed = true;
         notify();
         if (log.isTraceEnabled()) {
            log.tracef("Mark transaction committed: %s", this);
         }
      }

      public synchronized void waitUntilCommitted() throws InterruptedException {
         if (log.isTraceEnabled()) {
            log.tracef("[%s] will wait until this [%s] is committed", Thread.currentThread().getName(), this);
         }
         while (!committed) {
            wait();
         }
         if (log.isTraceEnabled()) {
            log.tracef("[%s] finishes waiting. %s is committed", Thread.currentThread().getName(), this);
         }
      }

      @Override
      public String toString() {
         return "TransactionEntry{" +
               "index=" + index +
               ", version=" + getVersion() +
               ", ready=" + ready +
               ", headStartTs=" + headStartTs +
               ", gtx=" + cacheTransaction.getGlobalTransaction().prettyPrint() +
               '}';
      }

      @Override
      public int compareTo(TransactionEntry transactionEntry) {
         if (transactionEntry == null) {
            return -1;
         }
         Long my = getVersion().getThisNodeVersionValue();
         Long other = transactionEntry.getVersion().getThisNodeVersionValue();
         int compareResult = my.compareTo(other);

         if (log.isTraceEnabled()) {
            log.tracef("Comparing this[%s] with other[%s]. compare(%s,%s) ==> %s", this, transactionEntry, my, other,
                       compareResult);
         }

         return compareResult;
      }
   }

   public static interface CommitInstance {
      void commitTransaction(TxInvocationContext ctx);
   }

   private class CurrentVersion {
      private IncrementableEntryVersion currentVersion = null;

      private CurrentVersion() {}

      public synchronized final void init() {
         if (log.isTraceEnabled()) {
            log.tracef("Init current version: %s", this);
         }
         currentVersion = versionGenerator.generateNew();
      }

      /*public synchronized final EntryVersion getCurrentVersion() {
         if (log.isTraceEnabled()) {
            log.tracef("Get current version: %s", this);
         }
         return currentVersion;
      }*/

      public synchronized final EntryVersion getNextPrepareVersion(EntryVersion prepareVersion) {
         update(prepareVersion);
         currentVersion = versionGenerator.increment(currentVersion);
         if (log.isTraceEnabled()) {
            log.tracef("[Prepare] Update current version: %s (with %s)", this, prepareVersion);
         }
         return currentVersion;
      }

      public synchronized final void update(EntryVersion version) {
         currentVersion = versionGenerator.mergeAndMax(Arrays.asList(currentVersion, version));
         if (log.isTraceEnabled()) {
            log.tracef("Update current version: %s (with %s)", this, version);
         }
      }

      @Override
      public String toString() {
         return "CurrentVersion{" +
               "currentVersion=" + currentVersion +
               '}';
      }
   }

   private class TransactionQueue {
      private final Map<GlobalTransaction, TransactionEntry> searchByTransaction;
      private final ArrayList<TransactionEntry> searchByIndex;


      private TransactionQueue() {
         this.searchByTransaction = new HashMap<GlobalTransaction, TransactionEntry>();
         this.searchByIndex = new ArrayList<TransactionEntry>();
      }

      public synchronized final void addTransaction(CacheTransaction cacheTransaction) {
         GlobalTransaction globalTransaction = cacheTransaction.getGlobalTransaction();
         if (searchByTransaction.containsKey(globalTransaction)) {
            throw new IllegalStateException("Transaction " + globalTransaction.prettyPrint() + " already exists");
         }
         EntryVersion preparedVersion = currentVersion.getNextPrepareVersion(cacheTransaction.getTransactionVersion());
         cacheTransaction.setTransactionVersion(preparedVersion);
         TransactionEntry transactionEntry = new TransactionEntry(cacheTransaction);

         searchByTransaction.put(globalTransaction, transactionEntry);
         sortInsert(transactionEntry);

         if (log.isTraceEnabled()) {
            log.tracef("Add %s to transaction queue: %s", globalTransaction.prettyPrint(), searchByIndex);
         }
      }

      public synchronized final void updateTransaction(CacheTransaction cacheTransaction) {
         GlobalTransaction globalTransaction = cacheTransaction.getGlobalTransaction();
         GMUEntryVersion commitVersion = toGMUEntryVersion(cacheTransaction.getTransactionVersion());
         currentVersion.update(commitVersion);
         TransactionEntry transactionEntry = searchByTransaction.get(globalTransaction);
         if (transactionEntry == null) {
            //read only?
            if (log.isTraceEnabled()) {
               log.tracef("Update %s in transaction queue but it was not found!", globalTransaction.prettyPrint());
            }
            return;
         }

         transactionEntry.commitVersion(commitVersion);
         notify();

         if (!isOutOfOrder(transactionEntry)) {
            if (log.isTraceEnabled()) {
               log.tracef("Update %s in transaction queue and the order didn't changed: %s",
                          globalTransaction.prettyPrint(), searchByIndex);
            }
            return;
         }

         //need to re-order
         removeAndShift(transactionEntry);
         transactionEntry.incrementIndex();
         sortInsert(transactionEntry);
         if (log.isTraceEnabled()) {
            log.tracef("Update %s in transaction queue and the order has changed: %s",
                       globalTransaction.prettyPrint(), searchByIndex);
         }
      }

      public synchronized void getTransactionsReadyToCommit(List<TransactionEntry> commitList)
            throws InterruptedException {
         if (log.isTraceEnabled()) {
            log.tracef("Get next transactions to commit");
         }
         if (searchByIndex.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("Queue is empty. Returning...");
            }
            wait();
            return;
         }

         int index = searchByIndex.size();

         TransactionEntry transactionEntry = searchByIndex.get(0);
         if (!transactionEntry.isReady()) {
            if (log.isTraceEnabled()) {
               log.tracef("First transaction [%s] is not ready to commit", transactionEntry);
            }
            wait();
            return;
         }

         for (TransactionEntry other : searchByIndex) {
            boolean sameVersion = transactionEntry.compareTo(other) == 0;
            if (sameVersion && !other.isReady()) {
               if (log.isTraceEnabled()) {
                  log.tracef("Two or more transaction has the same version but one of them is not ready to commit (%s)",
                             other);
               }
               wait();
               return;
            } else if (sameVersion) {
               if (log.isTraceEnabled()) {
                  log.tracef("Next transaction has the same version and it is ready to commit (%s)",
                             other);
               }
               continue;
            }
            if (log.isTraceEnabled()) {
               log.tracef("Next transaction has another version (%s)",
                          other);
            }
            index = other.getIndex();
            break;
         }

         Collection<TransactionEntry> subList = searchByIndex.subList(0, index);
         commitList.addAll(subList);
         subList.clear();
         committingTransactions(commitList);
         if (log.isTraceEnabled()) {
            log.tracef("Transactions ready to commit are: %s\nQueue: %s", commitList, searchByIndex);
         }
      }

      public synchronized void remove(GlobalTransaction globalTransaction) {
         TransactionEntry transactionEntry = searchByTransaction.remove(globalTransaction);
         if (transactionEntry == null) {
            return;
         }
         removeAndShift(transactionEntry);
         notify();
      }

      public synchronized TransactionEntry getTransactionEntry(GlobalTransaction globalTransaction) {
         return searchByTransaction.get(globalTransaction);
      }

      private void sortInsert(TransactionEntry transactionEntry) {
         int index = transactionEntry.getIndex();
         while (index < searchByIndex.size()) {
            TransactionEntry other = searchByIndex.get(index);
            if (transactionEntry.compareTo(other) <= 0) {
               transactionEntry.setIndex(index);
               addAndShift(transactionEntry);
               return;
            }
            index++;
         }
         transactionEntry.setIndex(index);
         addAndShift(transactionEntry);
      }

      private void committingTransactions(Collection<TransactionEntry> commitList) {
         for (TransactionEntry transactionEntry : commitList) {
            searchByTransaction.remove(transactionEntry.getGlobalTransaction());
         }
         for (TransactionEntry transactionEntry : searchByIndex) {
            transactionEntry.decrementIndex(commitList.size());
         }
      }

      private void removeAndShift(TransactionEntry toRemove) {
         int index = toRemove.getIndex();
         searchByIndex.remove(index);
         while (index < searchByIndex.size()) {
            searchByIndex.get(index++).decrementIndex(1);
         }
      }

      private void addAndShift(TransactionEntry transactionEntry) {
         searchByIndex.add(transactionEntry.getIndex(), transactionEntry);
         int index = transactionEntry.getIndex() + 1;
         while (index < searchByIndex.size()) {
            searchByIndex.get(index++).incrementIndex();
         }
      }

      private boolean isOutOfOrder(TransactionEntry transactionEntry) {
         if (transactionEntry.getIndex() == searchByIndex.size() - 1) {
            //is the last one
            return false;
         }
         TransactionEntry next = searchByIndex.get(transactionEntry.getIndex() + 1);
         //is the next one is lower than us, it is out of order
         return transactionEntry.compareTo(next) > 0;
      }
   }

   private class CommitThread extends Thread {
      private boolean running;
      private final List<GMUEntryVersion> committedVersions;
      private final List<TransactionEntry> commitList;

      private CommitThread() {
         super("GMU-Commit-Thread");
         running = false;
         committedVersions = new LinkedList<GMUEntryVersion>();
         commitList = new LinkedList<TransactionEntry>();
      }

      @Override
      public void run() {
         running = true;
         while (running) {
            try {
               transactionQueue.getTransactionsReadyToCommit(commitList);
               if (commitList.isEmpty()) {
                  continue;
               }

               for(TransactionEntry transactionEntry : commitList) {
                  try {
                     if (log.isTraceEnabled()) {
                        log.tracef("Committing transaction entries for %s", transactionEntry);
                     }

                     commitInvocationInstance.commitTransaction(createInvocationContext(transactionEntry));
                     committedVersions.add(transactionEntry.getVersion());

                     if (log.isTraceEnabled()) {
                        log.tracef("Transaction entries committed for %s", transactionEntry);
                     }
                  } catch (Exception e) {
                     log.warnf("Error occurs while committing transaction entries for %s", transactionEntry);
                  } finally {
                     icc.clearThreadLocal();
                     transactionEntry.committed();
                  }
               }

               commitLog.addNewVersion(versionGenerator.mergeAndMax(committedVersions));
            } catch (InterruptedException e) {
               if (log.isTraceEnabled()) {
                  log.tracef("%s was interrupted", getName());
               }
               this.interrupt();
            } finally {
               committedVersions.clear();
               commitList.clear();
            }
         }
      }

      @Override
      public void interrupt() {
         running = false;
         super.interrupt();
      }

      private TxInvocationContext createInvocationContext(TransactionEntry transactionEntry) {
         CacheTransaction cacheTransaction = transactionEntry.getCacheTransaction();

         if (cacheTransaction instanceof LocalTransaction) {
            LocalTransaction localTransaction = (LocalTransaction) cacheTransaction;
            localTransaction.setTransactionVersion(transactionEntry.getVersion());

            LocalTxInvocationContext localTxInvocationContext = icc.createTxInvocationContext();
            localTxInvocationContext.setLocalTransaction(localTransaction);

            return localTxInvocationContext;
         } else if (cacheTransaction instanceof RemoteTransaction) {
            RemoteTransaction remoteTransaction = (RemoteTransaction) cacheTransaction;
            remoteTransaction.setTransactionVersion(transactionEntry.getVersion());

            return icc.createRemoteTxInvocationContext(remoteTransaction, null);
         }
         throw new IllegalStateException("Expected a remote or local transaction and not " + cacheTransaction);
      }
   }
}
