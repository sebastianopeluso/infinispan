package org.infinispan.interceptors.serializable;

import org.infinispan.CacheException;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.SerializablePrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.AbstractTxLockingInterceptor;
import org.infinispan.util.TimSort;
import org.infinispan.util.concurrent.locks.readwritelock.ReadWriteLockManager;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SerializableLockingInterceptor extends AbstractTxLockingInterceptor {
   //Note: we don't need to override the visitCommit and visitRollback, because the abstract class releases the locks

   private static final MurmurHash3 HASH = new MurmurHash3();
   private final static Comparator<Object> keyComparator = new Comparator<Object>() {
      @Override
      public int compare(Object o1, Object o2) {
         int thisVal = HASH.hash(o1);
         int anotherVal = HASH.hash(o2);
         return (thisVal<anotherVal ? -1 : (thisVal==anotherVal ? 0 : 1));
      }
   };

   private final LockAcquisitionVisitor lockAcquisitionVisitor = new LockAcquisitionVisitor();

   private ReadWriteLockManager readWriteLockManager;
   private DataContainer dataContainer;

   @Inject
   public void inject(ReadWriteLockManager readWriteLockManager, DataContainer dataContainer) {
      this.readWriteLockManager = readWriteLockManager;
      this.dataContainer = dataContainer;
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new CacheException("Explicit locking is not allowed with optimistic caches!");
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object[] writeSet = getWriteSetSorter(command.getModifications());

      try{
         if (writeSet != null) {
            Object[] readSet = getReadSetSorted(Arrays.asList(((SerializablePrepareCommand)command).getReadSet()));
            acquireWriteLocks(ctx, writeSet);
            acquireReadLocks(ctx, readSet);
         } else {
            //it has the clear command. all the locks are acquired for write
            for (WriteCommand wc : command.getModifications()) {
               wc.acceptVisitor(ctx, lockAcquisitionVisitor);
            }
         }
      } catch (Throwable e) {
         throw cleanLocksAndRethrow(ctx, e);
      }

      return invokeNextAndCommitIf1Pc(ctx, command);
   }

   /**
    *
    * @param keys keys to check
    * @return return only the local key or empty if it has no one
    */
   protected Set<Object> getOnlyLocalKeys(Collection<Object> keys) {
      return new HashSet<Object>(keys);
   }

   @SuppressWarnings("UnusedParameters")
   protected boolean isKeyLocal(Object key) {
      return true;
   }

   private Object[] getWriteSetSorter(WriteCommand[] modifications) {
      Set<Object> set = new HashSet<Object>();
      for (WriteCommand wc : modifications) {
         switch (wc.getCommandId()) {
            case ClearCommand.COMMAND_ID:
               return null;
            case PutKeyValueCommand.COMMAND_ID:
            case RemoveCommand.COMMAND_ID:
            case ReplaceCommand.COMMAND_ID:
               set.add(((DataWriteCommand) wc).getKey());
               break;
            case PutMapCommand.COMMAND_ID:
               set.addAll(wc.getAffectedKeys());
               break;
            case ApplyDeltaCommand.COMMAND_ID:
               ApplyDeltaCommand command = (ApplyDeltaCommand) wc;
               if (isKeyLocal(command.getKey())) {
                  Object[] compositeKeys = command.getCompositeKeys();
                  set.addAll(Arrays.asList(compositeKeys));
               }
               break;
         }
      }

      Set<Object> localKeys = getOnlyLocalKeys(set);

      Object[] sorted = localKeys.toArray(new Object[localKeys.size()]);
      TimSort.sort(sorted, keyComparator);
      return sorted;
   }

   private Object[] getReadSetSorted(Collection<Object> readSet) {
      Set<Object> localKeys = getOnlyLocalKeys(readSet);
      Object[] sorted = localKeys.toArray(new Object[localKeys.size()]);
      TimSort.sort(sorted, keyComparator);
      return sorted;
   }

   private void acquireWriteLocks(TxInvocationContext ctx, Object[] writeSet) throws InterruptedException {
      for (Object key : writeSet) {
         acquireWriteLock(ctx, key);
      }
   }

   private void acquireReadLocks(TxInvocationContext ctx, Object[] readSet) throws InterruptedException {
      for (Object key : readSet) {
         acquireReadLock(ctx, key);
      }
   }

   private void acquireWriteLock(TxInvocationContext ctx, Object key) throws InterruptedException {
      if (cdl.localNodeIsPrimaryOwner(key)) {
         readWriteLockManager.acquireLock(ctx, key);
      } else if (cdl.localNodeIsOwner(key)) {
         ctx.getCacheTransaction().addBackupLockForKey(key);
      }
   }

   private void acquireReadLock(TxInvocationContext ctx, Object key) throws InterruptedException {
      if (cdl.localNodeIsPrimaryOwner(key)) {
         readWriteLockManager.acquireSharedLock(ctx, key);
      } else if (cdl.localNodeIsOwner(key)) {
         ctx.getCacheTransaction().addBackupLockForKey(key);
      }
   }

   private class LockAcquisitionVisitor extends AbstractVisitor {
      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         for (Object key : dataContainer.keySet()) {
            acquireWriteLock(txC, key);
            txC.addAffectedKey(key);
         }
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         for (Object key : command.getMap().keySet()) {
            acquireWriteLock(txC, key);
            txC.addAffectedKey(key);
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         acquireWriteLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         acquireWriteLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (isKeyLocal(command.getKey())) {
            Object[] compositeKeys = command.getCompositeKeys();
            TxInvocationContext txC = (TxInvocationContext) ctx;
            for (Object key : compositeKeys) {
               acquireWriteLock(txC, key);
            }
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         acquireWriteLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }
   }
}
