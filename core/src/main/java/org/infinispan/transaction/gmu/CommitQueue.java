package org.infinispan.transaction.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
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

   //AFTER THE VersionVCFactory
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
    * @param context the context    
    * @return the prepare vector clock
    */
   public EntryVersion prepareTransaction(GlobalTransaction globalTransaction, TxInvocationContext context) {
      return transactionQueue.addTransaction(globalTransaction, context);
   }

   public void rollbackTransaction(GlobalTransaction globalTransaction) {
      transactionQueue.remove(globalTransaction);
   }

   public void commitTransaction(GlobalTransaction globalTransaction, EntryVersion commitVersion, boolean wait)
         throws InterruptedException {
      TransactionEntry entry = transactionQueue.updateTransaction(globalTransaction, commitVersion);
      if (wait && entry != null) {
         entry.waitUntilCommitted();
      }
   }

   public EntryVersion prepareReadOnlyTransaction(TxInvocationContext context) {
      return currentVersion.getNextPrepareVersion(context.getTransactionVersion());
   }

   private static class TransactionEntry implements Comparable<TransactionEntry> {
      private int index;
      private GMUEntryVersion version;
      private final TxInvocationContext context;
      private final GlobalTransaction globalTransaction;
      private boolean ready;
      private boolean committed;
      private long headStartTs;

      private TransactionEntry(TxInvocationContext context, GMUEntryVersion prepareVersion,
                               GlobalTransaction globalTransaction) {
         this.context = context;
         this.globalTransaction = globalTransaction;
         this.index = 0;
         this.version = prepareVersion;
         this.ready = false;
         this.committed = false;
         this.headStartTs = 0;
      }

      public synchronized void setIndex(int index) {
         this.index = index;
         if (index == 0) {
            setHeadStartTs();
         }
      }

      public synchronized void incrementIndex() {
         index++;
      }

      public synchronized void decrementIndex(int decrement) {
         index -= decrement;
         if (index == 0) {
            setHeadStartTs();
         }
      }

      public synchronized int getIndex() {
         return index;
      }

      public synchronized void commitVersion(GMUEntryVersion entryVersion) {
         version = entryVersion;
         ready = true;
      }

      public synchronized GMUEntryVersion getVersion() {
         return version;
      }

      public TxInvocationContext getContext() {
         return context;
      }

      public synchronized boolean isReady() {
         return ready;
      }

      public GlobalTransaction getGlobalTransaction() {
         return globalTransaction;
      }

      public void setHeadStartTs() {
         this.headStartTs = System.nanoTime();
      }

      public long getHeadStartTs() {
         return headStartTs;
      }

      public synchronized void committed() {
         committed = true;
         notify();
      }

      public synchronized void waitUntilCommitted() throws InterruptedException {
         while (!committed) {
            wait();
         }
      }

      @Override
      public String toString() {
         return "TransactionEntry{" +
               "index=" + index +
               ", version=" + version +
               ", ready=" + ready +
               ", headStartTs=" + headStartTs +
               '}';
      }

      @Override
      public int compareTo(TransactionEntry transactionEntry) {
         if (transactionEntry == null) {
            return -1;
         }
         Long my = version.getThisNodeVersionValue();
         Long other = transactionEntry.version.getThisNodeVersionValue();
         return my.compareTo(other);
      }
   }

   public static interface CommitInstance {
      void commitContextEntries(TxInvocationContext ctx, GMUEntryVersion commitVersion);
   }

   private class CurrentVersion {
      private IncrementableEntryVersion currentVersion = null;

      private CurrentVersion() {}

      public synchronized final void init() {
         currentVersion = versionGenerator.generateNew();
      }

      public synchronized final EntryVersion getCurrentVersion() {
         return currentVersion;
      }

      public synchronized final EntryVersion getNextPrepareVersion(EntryVersion prepareVersion) {
         update(prepareVersion);
         currentVersion = versionGenerator.increment(currentVersion);
         return currentVersion;
      }

      public synchronized final void update(EntryVersion version) {
         currentVersion = versionGenerator.mergeAndMax(Arrays.asList(currentVersion, version));
      }
   }

   private class TransactionQueue {
      private final Map<GlobalTransaction, TransactionEntry> searchByTransaction;
      private final ArrayList<TransactionEntry> searchByIndex;


      private TransactionQueue() {
         this.searchByTransaction = new HashMap<GlobalTransaction, TransactionEntry>();
         this.searchByIndex = new ArrayList<TransactionEntry>();
      }

      public synchronized final EntryVersion addTransaction(GlobalTransaction globalTransaction,
                                                            TxInvocationContext context) {
         if (searchByTransaction.containsKey(globalTransaction)) {
            throw new IllegalStateException("Transaction " + globalTransaction.prettyPrint() + " already exists");
         }
         EntryVersion preparedVersion = currentVersion.getNextPrepareVersion(context.getTransactionVersion());
         TransactionEntry transactionEntry = new TransactionEntry(context, toGMUEntryVersion(preparedVersion),
                                                                  globalTransaction);

         searchByTransaction.put(globalTransaction, transactionEntry);
         sortInsert(transactionEntry);
         return preparedVersion;
      }

      public synchronized final TransactionEntry updateTransaction(GlobalTransaction globalTransaction,
                                                                   EntryVersion commitVersion) {
         currentVersion.update(commitVersion);
         TransactionEntry transactionEntry = searchByTransaction.get(globalTransaction);
         if (transactionEntry == null) {
            //read only?
            return null;
         }

         transactionEntry.commitVersion(toGMUEntryVersion(commitVersion));

         if (!isOutOfOrder(transactionEntry)) {
            return transactionEntry;
         }

         //need to re-order
         removeAndShift(transactionEntry);
         transactionEntry.incrementIndex();
         sortInsert(transactionEntry);
         notify();
         return transactionEntry;
      }

      public synchronized void getTransactionsReadyToCommit(List<TransactionEntry> commitList)
            throws InterruptedException {
         if (searchByIndex.isEmpty()) {
            wait();
            return;
         }

         int index = searchByIndex.size();

         TransactionEntry transactionEntry = searchByIndex.get(0);
         if (!transactionEntry.isReady()) {
            wait();
            return;
         }

         for (TransactionEntry other : searchByIndex) {
            boolean sameVersion = transactionEntry.compareTo(other) == 0;
            if (sameVersion && !other.isReady()) {
               wait();
               return;
            } else if (sameVersion) {
               continue;
            }
            index = other.getIndex();
            break;
         }

         Collection<TransactionEntry> subList = searchByIndex.subList(0, index);
         commitList.addAll(subList);
         subList.clear();
         committingTransactions(commitList);
      }

      public synchronized void remove(GlobalTransaction globalTransaction) {
         TransactionEntry transactionEntry = searchByTransaction.remove(globalTransaction);
         if (transactionEntry == null) {
            return;
         }
         removeAndShift(transactionEntry);
         notify();
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
            searchByIndex.get(index--).decrementIndex(1);
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
                  icc.setContext(transactionEntry.getContext());
                  commitInvocationInstance.commitContextEntries(transactionEntry.getContext(),
                                                                transactionEntry.getVersion());
                  transactionEntry.committed();
                  committedVersions.add(transactionEntry.getVersion());
                  icc.clearThreadLocal();
               }

               commitLog.addNewVersion(versionGenerator.mergeAndMax(committedVersions));
            } catch (InterruptedException e) {
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
   }
}
