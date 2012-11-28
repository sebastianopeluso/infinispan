package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullMarkerEntry;
import org.infinispan.container.entries.NullMarkerEntryForRemoval;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFlagsOverride;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.GMUHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.transaction.gmu.GMUHelper.toInternalGMUCacheEntry;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUEntryFactoryImpl extends EntryFactoryImpl {

   private CommitLog commitLog;
   private GMUVersionGenerator gmuVersionGenerator;

   private static final Log log = LogFactory.getLog(GMUEntryFactoryImpl.class);

   @Inject
   public void injectDependencies(CommitLog commitLog, VersionGenerator versionGenerator) {
      this.commitLog = commitLog;
      this.gmuVersionGenerator = GMUHelper.toGMUVersionGenerator(versionGenerator);
   }

   public void start() {
      useRepeatableRead = false;
      localModeWriteSkewCheck = false;
   }

   @Override
   protected MVCCEntry createWrappedEntry(Object key, Object value, EntryVersion version, boolean isForInsert, boolean forRemoval, long lifespan) {
      if (value == null && !isForInsert) {
         return forRemoval ? new NullMarkerEntryForRemoval(key, version) : NullMarkerEntry.getInstance();
      }
      return new SerializableEntry(key, value, lifespan, version);
   }

   @Override
   protected InternalCacheEntry getFromContainer(Object key, InvocationContext context) {
      boolean singleRead = context instanceof SingleKeyNonTxInvocationContext;
      boolean remotePrepare = !context.isOriginLocal() && context.isInTxScope();

      EntryVersion versionToRead;
      if (singleRead || remotePrepare) {
         //read the most recent version
         //in the prepare, the value does not matter (it will be written or it is not read)
         //                and the version does not matter either (it will be overwritten)
         versionToRead = null;
      } else {
         versionToRead = context.calculateVersionToRead(gmuVersionGenerator);
      }

      boolean hasAlreadyReadFromThisNode = context.hasAlreadyReadOnThisNode();

      EntryVersion realVersionToRead = hasAlreadyReadFromThisNode ? versionToRead :
            commitLog.getAvailableVersionLessThan(versionToRead);

      InternalGMUCacheEntry entry = toInternalGMUCacheEntry(container.get(key, realVersionToRead));

      context.addKeyReadInCommand(key, entry);

      if (log.isTraceEnabled()) {
         log.tracef("Retrieved from container %s", entry);
      }

      return entry.getInternalCacheEntry();
   }
}
