package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.AbstractExternalizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InternalGMUNullCacheEntry extends InternalGMURemovedCacheEntry {

   private final boolean mostRecent;
   private final EntryVersion creationVersion;
   private final EntryVersion maxValidVersion;
   private final EntryVersion maxTxVersion;

   public InternalGMUNullCacheEntry(Object key, EntryVersion version, EntryVersion creationVersion,
                                    EntryVersion maxValidVersion, EntryVersion maxTxVersion, boolean mostRecent) {
      super(key, version);
      this.creationVersion = creationVersion;
      this.maxValidVersion = maxValidVersion;
      this.maxTxVersion = maxTxVersion;
      this.mostRecent = mostRecent;
   }

   public InternalGMUNullCacheEntry(InternalGMUCacheEntry expired) {
      super(expired.getKey(), expired.getVersion());
      this.creationVersion = expired.getCreationVersion();
      this.maxValidVersion = expired.getMaximumValidVersion();
      this.maxTxVersion = expired.getMaximumTransactionVersion();
      this.mostRecent = expired.isMostRecent();
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new InternalGMUNullCacheValue(getVersion(), mostRecent, creationVersion, maxValidVersion, maxTxVersion);
   }

   @Override
   public EntryVersion getMaximumTransactionVersion() {
      return maxTxVersion;
   }

   @Override
   public EntryVersion getMaximumValidVersion() {
      return maxValidVersion;
   }

   @Override
   public EntryVersion getCreationVersion() {
      return creationVersion;
   }

   @Override
   public boolean isMostRecent() {
      return mostRecent;
   }

   @Override
   public String toString() {
      return "InternalGMUNullCacheEntry{" +
            "key=" + getKey() +
            ", version=" + getVersion() +
            ", value=" + getValue() +
            ", creationVersion=" + creationVersion +
            ", maxValidVersion=" + maxValidVersion +
            ", maxTxVersion=" + maxTxVersion +
            ", mostRecent=" + mostRecent +
            "}";
   }

   public static class Externalizer extends AbstractExternalizer<InternalGMUNullCacheEntry> {

      @Override
      public Set<Class<? extends InternalGMUNullCacheEntry>> getTypeClasses() {
         return null;  // TODO: Customise this generated block
      }

      @Override
      public void writeObject(ObjectOutput output, InternalGMUNullCacheEntry object) throws IOException {
         // TODO: Customise this generated block
      }

      @Override
      public InternalGMUNullCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return null;  // TODO: Customise this generated block
      }
   }
}
