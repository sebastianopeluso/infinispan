package org.infinispan.tx.gmu;

import org.infinispan.Cache;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.gmu.manager.TransactionCommitManager;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.transaction.gmu.manager.SortedTransactionQueue.TransactionEntry;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistConsistencyTest extends ConsistencyTest {

   public void testWaitingForLocalCommit() throws Exception {
      assertAtLeastCaches(2);

      DelayCommit delayCommit = new DelayCommit(5000);
      cache(0).getAdvancedCache().addInterceptorAfter(delayCommit, TxInterceptor.class);

      final ObtainTransactionEntry obtainTransactionEntry = new ObtainTransactionEntry(cache(1));

      final Object localKey = new GMUMagicKey(cache(0), cache(1), "LocalKey");
      final Object remoteKey = new GMUMagicKey(cache(1), cache(0), "RemoteKey");
      assertCacheValuesNull(localKey, remoteKey);

      tm(0).begin();
      txPut(0, localKey, VALUE_1, null);
      txPut(0, remoteKey, VALUE_1, null);
      tm(0).commit();

      Thread otherThread = new Thread("TestWaitingForLocalCommit-Thread") {
         @Override
         public void run() {
            try {
               tm(1).begin();
               txPut(1, localKey, VALUE_2, VALUE_1);
               txPut(1, remoteKey, VALUE_2, VALUE_1);
               obtainTransactionEntry.expectedThisThread();
               tm(1).commit();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };
      obtainTransactionEntry.reset();
      otherThread.start();
      TransactionEntry transactionEntry = obtainTransactionEntry.getTransactionEntry();
      transactionEntry.awaitUntilCommitted(null);

      tm(0).begin();
      assertEquals(VALUE_2, cache(0).get(remoteKey));
      assertEquals(VALUE_2, cache(0).get(localKey));
      tm(0).commit();

      delayCommit.unblock();
      otherThread.join();

      printDataContainer();
      assertNoTransactions();
      cache(0).getAdvancedCache().removeInterceptor(DelayCommit.class);
      cache(1).getAdvancedCache().removeInterceptor(ObtainTransactionEntry.class);
   }

   public void testWaitInRemoteNode() throws Exception {
      assertAtLeastCaches(2);

      DelayCommit delayCommit = new DelayCommit(5000);
      cache(0).getAdvancedCache().addInterceptorAfter(delayCommit, TxInterceptor.class);

      final ObtainTransactionEntry obtainTransactionEntry = new ObtainTransactionEntry(cache(1));

      final Object cache0Key = new GMUMagicKey(cache(0), cache(1), "Key0");
      final Object cache1Key = new GMUMagicKey(cache(1), cache(0), "Key1");
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();

      Thread otherThread = new Thread("TestWaitingForLocalCommit-Thread") {
         @Override
         public void run() {
            try {
               tm(1).begin();
               txPut(1, cache0Key, VALUE_2, VALUE_1);
               txPut(1, cache1Key, VALUE_2, VALUE_1);
               obtainTransactionEntry.expectedThisThread();
               tm(1).commit();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };
      obtainTransactionEntry.reset();
      otherThread.start();
      TransactionEntry transactionEntry = obtainTransactionEntry.getTransactionEntry();
      transactionEntry.awaitUntilCommitted(null);

      //tx already committed in cache(1). start a read only on cache(1) reading the local key and them the remote key.
      // the remote get should wait until the transaction is committed
      tm(1).begin();
      assertEquals(VALUE_2, cache(1).get(cache1Key));
      assertEquals(VALUE_2, cache(1).get(cache0Key));
      tm(1).commit();

      delayCommit.unblock();
      otherThread.join();

      printDataContainer();
      assertNoTransactions();
      cache(0).getAdvancedCache().removeInterceptor(DelayCommit.class);
      cache(1).getAdvancedCache().removeInterceptor(ObtainTransactionEntry.class);
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }

   private class DelayCommit extends CommandInterceptor {
      private final long delay;

      private DelayCommit(long delay) {
         this.delay = delay;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         await();
         return invokeNextInterceptor(ctx, command);
      }

      public synchronized void await() {
         try {
            wait(delay);
         } catch (Exception e) {
            //ignore
         }
      }

      public synchronized void unblock() {
         notify();
      }
   }

   private class ObtainTransactionEntry extends BaseCustomInterceptor {
      private final TransactionCommitManager transactionCommitManager;
      private TransactionEntry transactionEntry;
      private Thread expectedThread;

      public ObtainTransactionEntry(Cache<?,?> cache) {
         this.transactionCommitManager = cache.getAdvancedCache().getComponentRegistry()
               .getComponent(TransactionCommitManager.class);
         cache.getAdvancedCache().addInterceptorAfter(this, TxInterceptor.class);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         setTransactionEntry(transactionCommitManager.getTransactionEntry(command.getGlobalTransaction()));
         return invokeNextInterceptor(ctx, command);
      }

      private synchronized void setTransactionEntry(TransactionEntry transactionEntry) {
         if (Thread.currentThread().equals(expectedThread)) {
            this.transactionEntry = transactionEntry;
            notifyAll();
         }
      }

      public synchronized TransactionEntry getTransactionEntry() throws InterruptedException {
         while (transactionEntry == null) {
            wait();
         }
         return transactionEntry;
      }

      public synchronized void expectedThisThread() {
         this.expectedThread = Thread.currentThread();
      }

      public synchronized void reset() {
         transactionEntry = null;
      }
   }
}
