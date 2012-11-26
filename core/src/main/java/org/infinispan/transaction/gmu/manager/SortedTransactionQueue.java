package org.infinispan.transaction.gmu.manager;

import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.container.versioning.gmu.GMUEntryVersion;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUEntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SortedTransactionQueue {

   private static final Log log = LogFactory.getLog(SortedTransactionQueue.class);

   private final ConcurrentHashMap<GlobalTransaction, Node> concurrentHashMap;
   private final Node firstEntry;
   private final Node lastEntry;

   public SortedTransactionQueue() {
      this.concurrentHashMap = new ConcurrentHashMap<GlobalTransaction, Node>();

      this.firstEntry = new Node() {
         private Node first;

         @Override
         public void commitVersion(GMUEntryVersion commitCommand) {}

         @Override
         public GMUEntryVersion getVersion() {
            throw new IllegalStateException("Cannot return the version from the first node");
         }

         @Override
         public boolean isReady() {
            return false;
         }

         @Override
         public GlobalTransaction getGlobalTransaction() {
            throw new IllegalStateException("Cannot return the global transaction from the first node");
         }

         @Override
         public Node getPrevious() {
            throw new IllegalStateException("Cannot return the previous node from the first node");
         }

         @Override
         public void setPrevious(Node previous) {
            throw new IllegalStateException("Cannot set the previous node from the first node");
         }

         @Override
         public Node getNext() {
            return first;
         }

         @Override
         public void setNext(Node next) {
            this.first = next;
         }

         @Override
         public int compareTo(Node o) {
            //the first node is always lower
            return -1;
         }

         @Override
         public void awaitUntilCommitted(GMUCommitCommand commitCommand) throws InterruptedException {}

         @Override
         public CacheTransaction getCacheTransactionForCommit() {
            throw new IllegalStateException("Cannot return the cache transaction from the first node");
         }

         @Override
         public void committed() {}
      };

      this.lastEntry = new Node() {
         private Node last;

         @Override
         public void commitVersion(GMUEntryVersion commitCommand) {}

         @Override
         public GMUEntryVersion getVersion() {
            throw new IllegalStateException("Cannot return the version from the last node");
         }

         @Override
         public boolean isReady() {
            return false;
         }

         @Override
         public GlobalTransaction getGlobalTransaction() {
            throw new IllegalStateException("Cannot return the global transaction from the last node");
         }

         @Override
         public Node getPrevious() {
            return last;
         }

         @Override
         public void setPrevious(Node previous) {
            this.last = previous;
         }

         @Override
         public Node getNext() {
            throw new IllegalStateException("Cannot return the next node from the last node");
         }

         @Override
         public void setNext(Node next) {
            throw new IllegalStateException("Cannot set the next node from the last node");
         }

         @Override
         public int compareTo(Node o) {
            //the last node is always higher
            return 1;
         }

         @Override
         public void awaitUntilCommitted(GMUCommitCommand commitCommand) throws InterruptedException {}

         @Override
         public CacheTransaction getCacheTransactionForCommit() {
            throw new IllegalStateException("Cannot return the cache transaction from the last node");
         }

         @Override
         public void committed() {}
      };

      firstEntry.setNext(lastEntry);
      lastEntry.setPrevious(firstEntry);
   }

   public final void prepare(CacheTransaction cacheTransaction) {
      GlobalTransaction globalTransaction = cacheTransaction.getGlobalTransaction();
      if (concurrentHashMap.contains(globalTransaction)) {
         log.warnf("Duplicated prepare for %s", globalTransaction);
      }
      Node entry = new TransactionEntryImpl(cacheTransaction);
      concurrentHashMap.put(globalTransaction, entry);
      addNew(entry);
   }

   public final void rollback(CacheTransaction cacheTransaction) {
      remove(concurrentHashMap.remove(cacheTransaction.getGlobalTransaction()));
      notifyIfNeeded();
   }

   public final void commit(CacheTransaction cacheTransaction, GMUEntryVersion commitVersion) {
      Node entry = concurrentHashMap.get(cacheTransaction.getGlobalTransaction());
      if (entry == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Cannot commit transaction %s. Maybe it is a read-only on this node",
                       cacheTransaction.getGlobalTransaction().prettyPrint());
         }
         return;
      }
      update(entry, commitVersion);
      notifyIfNeeded();
   }

   public final synchronized void populateToCommit(List<TransactionEntry> transactionEntryList) throws InterruptedException {
      while (!firstEntry.getNext().isReady()) {
         wait();
      }

      Node firstTransaction = firstEntry.getNext();

      Node transactionToCheck = firstTransaction.getNext();

      while (transactionToCheck != lastEntry) {
         boolean isSameVersion = transactionToCheck.compareTo(firstTransaction) == 0;
         if (!isSameVersion) {
            //commit until this transaction
            commitUntil(transactionToCheck, transactionEntryList);
            return;
         } else if (!transactionToCheck.isReady()) {
            //has the same version and it is not ready
            wait();
            return;
         }
      }
      //commit until this transaction
      commitUntil(transactionToCheck, transactionEntryList);
   }

   public final TransactionEntry getTransactionEntry(GlobalTransaction globalTransaction) {
      return concurrentHashMap.get(globalTransaction);
   }

   private void commitUntil(Node exclusive, List<TransactionEntry> transactionEntryList) {
      Node transaction = firstEntry.getNext();
      exclusive.getPrevious().setNext(null);
      exclusive.setPrevious(firstEntry);

      while (transaction != null) {
         concurrentHashMap.remove(transaction.getGlobalTransaction());
         transactionEntryList.add(transaction);
         transaction.getPrevious().setNext(null);
         transaction.setPrevious(null);
         transaction = transaction.getNext();
      }

      firstEntry.setNext(exclusive);
   }

   private synchronized void update(Node entry, GMUEntryVersion commitVersion) {
      entry.commitVersion(commitVersion);
      if (entry.compareTo(entry.getNext()) > 0) {
         Node insertBefore = entry.getNext().getNext();
         remove(entry);
         while (entry.compareTo(insertBefore) > 0) {
            insertBefore = insertBefore.getNext();
         }
         addBefore(insertBefore, entry);
      }
   }

   private synchronized void addNew(Node entry) {
      Node insertAfter = lastEntry.getPrevious();

      while (insertAfter != firstEntry) {
         if (insertAfter.compareTo(entry) <= 0) {
            break;
         }
         insertAfter = insertAfter.getPrevious();
      }
      addAfter(insertAfter, entry);
   }

   private synchronized void remove(Node entry) {
      if (entry == null) {
         return;
      }
      Node previous = entry.getPrevious();
      Node next = entry.getNext();
      entry.setPrevious(null);
      entry.setNext(null);

      previous.setNext(next);
      next.setPrevious(previous);
   }

   private synchronized void addAfter(Node insertAfter, Node entry) {
      entry.setNext(insertAfter.getNext());
      insertAfter.getNext().setPrevious(entry);

      entry.setPrevious(insertAfter);
      insertAfter.setNext(entry);
   }

   private void addBefore(Node insertBefore, Node entry) {
      entry.setPrevious(insertBefore.getPrevious());
      insertBefore.getPrevious().setNext(entry);

      entry.setNext(insertBefore);
      insertBefore.setPrevious(entry);
   }

   private synchronized void notifyIfNeeded() {
      if (firstEntry.getNext().isReady()) {
         notify();
      }
   }

   private class TransactionEntryImpl implements Node {

      private final CacheTransaction cacheTransaction;
      private GMUEntryVersion entryVersion;
      private boolean ready;
      private boolean committed;
      private GMUCommitCommand commitCommand;

      private Node previous;
      private Node next;

      private TransactionEntryImpl(CacheTransaction cacheTransaction) {
         this.cacheTransaction = cacheTransaction;
         this.entryVersion = toGMUEntryVersion(cacheTransaction.getTransactionVersion());
      }

      public synchronized void commitVersion(GMUEntryVersion commitVersion) {
         this.entryVersion = commitVersion;
         this.ready = true;
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

      public synchronized void committed() {
         if (log.isTraceEnabled()) {
            log.tracef("Mark transaction committed: %s", this);
         }
         committed = true;
         if (commitCommand != null) {
            commitCommand.sendReply(null, false);
         }
      }

      @Override
      public synchronized void awaitUntilCommitted(GMUCommitCommand commitCommand) throws InterruptedException {
         if (committed && commitCommand != null) {
            commitCommand.sendReply(null, false);
            return;
         }
         if (commitCommand != null) {
            this.commitCommand = commitCommand;
            return;
         }
         while (!committed) {
            wait();
         }
      }

      @Override
      public CacheTransaction getCacheTransactionForCommit() {
         cacheTransaction.setTransactionVersion(entryVersion);
         return cacheTransaction;
      }

      @Override
      public String toString() {
         return "TransactionEntry{" +
               ", version=" + getVersion() +
               ", ready=" + isReady() +
               ", gtx=" + cacheTransaction.getGlobalTransaction().prettyPrint() +
               '}';
      }

      @Override
      public int compareTo(Node otherNode) {
         if (otherNode == null) {
            return -1;
         } else if (otherNode == firstEntry) {
            return 1;
         } else if (otherNode == lastEntry) {
            return -1;
         }

         Long my = getVersion().getThisNodeVersionValue();
         Long other = otherNode.getVersion().getThisNodeVersionValue();
         int compareResult = my.compareTo(other);

         if (log.isTraceEnabled()) {
            log.tracef("Comparing this[%s] with other[%s]. compare(%s,%s) ==> %s", this, otherNode, my, other,
                       compareResult);
         }

         return compareResult;
      }

      @Override
      public Node getPrevious() {
         return previous;
      }

      @Override
      public void setPrevious(Node previous) {
         this.previous = previous;
      }

      @Override
      public Node getNext() {
         return next;
      }

      @Override
      public void setNext(Node next) {
         this.next = next;
      }
   }

   private interface Node extends TransactionEntry, Comparable<Node> {
      void commitVersion(GMUEntryVersion commitCommand);
      GMUEntryVersion getVersion();
      boolean isReady();
      GlobalTransaction getGlobalTransaction();

      Node getPrevious();
      void setPrevious(Node previous);

      Node getNext();
      void setNext(Node next);
   }

   public static interface TransactionEntry {
      void awaitUntilCommitted(GMUCommitCommand commitCommand) throws InterruptedException;
      CacheTransaction getCacheTransactionForCommit();
      void committed();
   }
}
