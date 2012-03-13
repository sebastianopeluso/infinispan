package org.infinispan.interceptors.totalorder;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.VersionedEntryWrappingInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Wrapping Interceptor for Total Order protocol with versioning
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class TotalOrderVersionedEntryWrappingInterceptor extends VersionedEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedEntryWrappingInterceptor.class);

   private boolean trace;

   @Start
   public void setLogLevel() {
      trace = log.isTraceEnabled();
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {

      if (ctx.isOriginLocal())
         return invokeNextInterceptor(ctx, command);

      VersionCheckWrappingEntryVisitor visitor = new VersionCheckWrappingEntryVisitor(command);

      //both wraps and performs the write skew check
      if (!ctx.isOriginLocal() || command.isReplayEntryWrapping()) {
         for (WriteCommand c : command.getModifications())
            c.acceptVisitor(ctx, visitor);
      }

      Object retVal = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit()) {
         commitContextEntries(ctx);
      } else {
         if (trace)
            log.tracef("Transaction %s will be committed in the 2nd phase", ctx.getGlobalTransaction().prettyPrint());
      }

      return retVal;
   }

   @Override
   protected final void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      if (ctx.isInTxScope()) {
         ClusteredRepeatableReadEntry clusterMvccEntry = (ClusteredRepeatableReadEntry) entry;
         EntryVersion existingVersion = clusterMvccEntry.getVersion();

         EntryVersion newVersion;
         if (existingVersion == null) {
            newVersion = versionGenerator.generateNew();
         } else {
            newVersion = versionGenerator.increment((IncrementableEntryVersion) existingVersion);
         }
         cll.commitEntry(entry, newVersion, skipOwnershipCheck);
      } else {
         // This could be a state transfer call!
         cll.commitEntry(entry, entry.getVersion(), skipOwnershipCheck);
      }
   }


   public class VersionCheckWrappingEntryVisitor extends EntryWrappingVisitor {

      final VersionedPrepareCommand prepareCommand;

      public VersionCheckWrappingEntryVisitor(PrepareCommand command) {
         this.prepareCommand = (VersionedPrepareCommand) command;
      }

      @Override
      protected final MVCCEntry wrapEntryForReplace(InvocationContext ctx, Object key) throws InterruptedException {
         return checkForWriteSkew(super.wrapEntryForReplace(ctx, key));
      }

      @Override
      protected final MVCCEntry wrapEntryForRemove(InvocationContext ctx, Object key) throws InterruptedException {
         return checkForWriteSkew(super.wrapEntryForRemove(ctx, key));
      }

      @Override
      protected final MVCCEntry wrapEntryForClear(InvocationContext ctx, Object key) throws InterruptedException {
         return checkForWriteSkew(super.wrapEntryForClear(ctx, key));
      }

      @Override
      protected final MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, boolean putIfAbsent) throws InterruptedException {
         return checkForWriteSkew(super.wrapEntryForPut(ctx, key, putIfAbsent));
      }

      private MVCCEntry checkForWriteSkew(MVCCEntry mvccEntry) {
         ClusteredRepeatableReadEntry clusterMvccEntry = (ClusteredRepeatableReadEntry) mvccEntry;

         EntryVersion versionSeen = prepareCommand.getVersionsSeen().get(clusterMvccEntry.getKey());

         if (versionSeen != null) {
            clusterMvccEntry.setVersion(versionSeen);
         }

         if(!clusterMvccEntry.performWriteSkewCheck(dataContainer)) {
            throw new CacheException("Write skew detected on key " + clusterMvccEntry.getKey() +
                                           " for transaction " + prepareCommand.getGlobalTransaction().prettyPrint());
         }
         return clusterMvccEntry;
      }
   }
}
