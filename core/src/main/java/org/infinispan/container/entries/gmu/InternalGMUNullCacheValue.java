package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InternalGMUNullCacheValue extends InternalGMURemovedCacheValue {

   private final boolean mostRecent;
   private final EntryVersion creationVersion;
   private final EntryVersion maxValidVersion;
   private final EntryVersion maxTxVersion;


   public InternalGMUNullCacheValue(EntryVersion version, boolean mostRecent, EntryVersion creationVersion,
                                    EntryVersion maxValidVersion, EntryVersion maxTxVersion) {
      super(version);
      this.mostRecent = mostRecent;
      this.creationVersion = creationVersion;
      this.maxValidVersion = maxValidVersion;
      this.maxTxVersion = maxTxVersion;
   }

   @Override
   public EntryVersion getCreationVersion() {
      return creationVersion;
   }

   @Override
   public EntryVersion getMaximumValidVersion() {
      return maxValidVersion;
   }

   @Override
   public EntryVersion getMaximumTransactionVersion() {
      return maxTxVersion;
   }

   @Override
   public boolean isMostRecent() {
      return mostRecent;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new InternalGMUNullCacheEntry(key, version, creationVersion, maxValidVersion, maxTxVersion, mostRecent);
   }
}
