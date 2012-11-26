package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InternalGMUValueCacheValue implements InternalGMUCacheValue {

   private final InternalCacheValue internalCacheValue;
   private final EntryVersion creationVersion;
   private final EntryVersion maxTxVersion;
   private final EntryVersion maxValidVersion;
   private final boolean mostRecent;

   public InternalGMUValueCacheValue(InternalCacheValue internalCacheValue, EntryVersion creationVersion, EntryVersion maxTxVersion, EntryVersion maxValidVersion, boolean mostRecent) {
      this.internalCacheValue = internalCacheValue;
      this.creationVersion = creationVersion;
      this.maxTxVersion = maxTxVersion;
      this.maxValidVersion = maxValidVersion;
      this.mostRecent = mostRecent;
   }

   @Override
   public Object getValue() {
      return internalCacheValue.getValue();
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new InternalGMUValueCacheEntry(internalCacheValue.toInternalCacheEntry(key), creationVersion, maxTxVersion, maxValidVersion, mostRecent);
   }

   @Override
   public boolean isExpired(long now) {
      return internalCacheValue.isExpired(now);
   }

   @Override
   public boolean isExpired() {
      return internalCacheValue.isExpired();
   }

   @Override
   public boolean canExpire() {
      return internalCacheValue.canExpire();
   }

   @Override
   public long getCreated() {
      return internalCacheValue.getCreated();
   }

   @Override
   public long getLastUsed() {
      return internalCacheValue.getLastUsed();
   }

   @Override
   public long getLifespan() {
      return internalCacheValue.getLifespan();
   }

   @Override
   public long getMaxIdle() {
      return internalCacheValue.getMaxIdle();
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
   public InternalCacheValue getInternalCacheValue() {
      return internalCacheValue;
   }
}
