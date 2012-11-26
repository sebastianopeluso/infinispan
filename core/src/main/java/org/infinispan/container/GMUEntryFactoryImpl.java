package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullMarkerEntry;
import org.infinispan.container.entries.NullMarkerEntryForRemoval;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.gmu.CommitLog;
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

   private static final Log log = LogFactory.getLog(GMUEntryFactoryImpl.class);

   @Inject
   public void injectDependencies(CommitLog commitLog) {
      this.commitLog = commitLog;
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
      EntryVersion versionToRead = context.calculateVersionToRead(null);
      boolean hasAlreadyReadFromThisNode = context.hasAlreadyReadOnThisNode();

      EntryVersion realVersionToRead = hasAlreadyReadFromThisNode ? versionToRead :
            commitLog.getAvailableVersionLessThan(versionToRead);

      InternalGMUCacheEntry entry = toInternalGMUCacheEntry(container.get(key, realVersionToRead));

      context.addKeyReadInCommand(key, entry);

      return entry.getInternalCacheEntry();
   }
}
