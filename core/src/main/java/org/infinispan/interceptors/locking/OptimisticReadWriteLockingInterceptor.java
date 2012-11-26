package org.infinispan.interceptors.locking;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.TimSort;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OptimisticReadWriteLockingInterceptor extends OptimisticLockingInterceptor {

   @Override
   protected void afterWriteLocksAcquired(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException {
      GMUPrepareCommand spc = (GMUPrepareCommand) command;
      Object[] readSet = spc.getReadSet();
      TimSort.sort(readSet, keyComparator);
      acquireReadLocks(ctx, readSet);
   }

   private void acquireReadLocks(TxInvocationContext ctx, Object[] readSet) throws InterruptedException {      
      for (Object key : readSet) {
         internalLockAndRegisterBackupLock(ctx, key, true);
         ctx.addAffectedKey(key);
      }
   }
}
