package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUNullCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMURemovedCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUValueCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.BEFORE;
import static org.infinispan.container.versioning.InequalVersionComparisonResult.BEFORE_OR_EQUAL;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUDataContainer extends AbstractDataContainer<GMUDataContainer.VersionChain> {


   protected GMUDataContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   protected GMUDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy) {
      super(concurrencyLevel, maxEntries, strategy, policy);
   }

   @Override
   public InternalCacheEntry get(Object k, EntryVersion version) {
      InternalCacheEntry entry = peek(k, version);
      long now = System.currentTimeMillis();
      if (entry.canExpire() && entry.isExpired(now)) {
         if (entry instanceof InternalGMUCacheEntry) {
            InternalGMUCacheEntry gmuCacheEntry = (InternalGMUCacheEntry) entry;
            return new InternalGMUNullCacheEntry(gmuCacheEntry);
         }
         throw new IllegalStateException("Entry should be an InternalGMUCacheEntry");
      }
      entry.touch(now);
      return entry;
   }

   @Override
   public InternalCacheEntry peek(Object k, EntryVersion version) {
      EntryVersion maxVersion = getVersionToRead(version);
      VersionChain chain = entries.get(k);
      if (chain == null) {
         return wrap(k, null, true, maxVersion);
      }
      GMUVersionEntry entry = chain.get(maxVersion);
      return wrap(k, entry.getEntry(), entry.isMostRecent(), maxVersion);
   }

   @Override
   public void put(Object k, Object v, EntryVersion version, long lifespan, long maxIdle) {
      VersionChain chain = entries.get(k);

      if (chain == null) {
         chain = new VersionChain();
         entries.put(k, chain);
      }

      EntryVersion maxVersion = getVersionToWrite(version);

      InternalCacheEntry e = chain.get(maxVersion).getEntry();
      if (e != null) {
         e.setValue(v);
         InternalCacheEntry original = e;
         e.setVersion(version);
         e = entryFactory.update(e, lifespan, maxIdle);
         // we have the same instance. So we need to reincarnate.
         if(original == e) {
            e.reincarnate();
         }
      } else {
         // this is a brand-new entry
         e = entryFactory.create(k, v, version, lifespan, maxIdle);
      }
      chain.add(e);
   }

   @Override
   public boolean containsKey(Object k, EntryVersion version) {
      VersionChain chain = entries.get(k);
      return chain != null && chain.contains(getVersionToRead(version));
   }

   @Override
   public InternalCacheEntry remove(Object k, EntryVersion version) {
      VersionChain chain = entries.get(k);
      if (chain == null) {
         return wrap(k, null, true, null);
      }
      GMUVersionEntry entry = chain.remove(k, getVersionToWrite(version));
      return wrap(k, entry.getEntry(), entry.isMostRecent(), null);
   }

   @Override
   public int size(EntryVersion version) {
      int size = 0;
      EntryVersion maxVersion = getVersionToRead(version);
      for (VersionChain chain : entries.values()) {
         if (chain.contains(maxVersion)) {
            size++;
         }
      }
      return size;
   }

   @Override
   public void clear() {
      entries.clear();
   }

   @Override
   public void clear(EntryVersion version) {
      for (Object key : entries.keySet()) {
         remove(key, version);
      }
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = System.currentTimeMillis();
      for (VersionChain chain : entries.values()) {
         chain.purgeExpired(currentTimeMillis);
      }
   }

   public static DataContainer boundedDataContainer(int concurrencyLevel, int maxEntries,
                                                    EvictionStrategy strategy, EvictionThreadPolicy policy) {
      return new GMUDataContainer(concurrencyLevel, maxEntries, strategy, policy);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel) {
      return new GMUDataContainer(concurrencyLevel);
   }

   @Override
   protected Map<Object, InternalCacheEntry> getCacheEntries(Map<Object, VersionChain> evicted) {
      Map<Object, InternalCacheEntry> evictedMap = new HashMap<Object, InternalCacheEntry>();
      for (Map.Entry<Object, VersionChain> entry : evicted.entrySet()) {
         evictedMap.put(entry.getKey(), entry.getValue().get(null).getEntry());
      }
      return evictedMap;
   }

   @Override
   protected InternalCacheEntry getCacheEntry(VersionChain evicted) {
      return evicted.get(null).getEntry();
   }

   @Override
   protected InternalCacheEntry getCacheEntry(VersionChain entry, EntryVersion version) {
      return entry == null ? null : entry.get(getVersionToRead(version)).getEntry();
   }

   @Override
   protected EntryIterator createEntryIterator(EntryVersion version) {
      return new GMUEntryIterator(getVersionToRead(version), entries.values().iterator());
   }

   private EntryVersion getVersionToRead(EntryVersion version) {
      //TODO, go to commit log and return the maximum version corresponding to this version
      return version;
   }

   private EntryVersion getVersionToWrite(EntryVersion version) {
      if (version == null) {
         throw new NullPointerException("Null version are not allowed to set to entry value");
      }
      //TODO, return the long needed
      return version;
   }

   private static boolean isOlder(VersionBody current, VersionBody toAdd) {
      return current.getInternalCacheEntry().getVersion().compareTo(toAdd.getInternalCacheEntry().getVersion()) == BEFORE;
   }

   private static boolean isOlderOrEqual(VersionBody current, EntryVersion version) {
      InequalVersionComparisonResult result = current.getInternalCacheEntry().getVersion().compareTo(version);
      return result == BEFORE || result == BEFORE_OR_EQUAL;
   }

   private static boolean isExpired(VersionBody body, long now) {
      InternalCacheEntry entry = body.getInternalCacheEntry();
      return entry != null && entry.canExpire() && entry.isExpired(now);
   }

   private static InternalCacheEntry wrap(Object key, InternalCacheEntry entry, boolean mostRecent,
                                          EntryVersion maxTxVersion) {
      if (entry == null || entry.isNull()) {
         return new InternalGMUNullCacheEntry(key, (entry == null ? null : entry.getVersion()), null, maxTxVersion, null, mostRecent);
      }
      return new InternalGMUValueCacheEntry(entry, null, maxTxVersion, null, mostRecent);
   }

   private class GMUEntryIterator extends EntryIterator {

      private final EntryVersion version;
      private final Iterator<VersionChain> iterator;
      private InternalCacheEntry next;

      private GMUEntryIterator(EntryVersion version, Iterator<VersionChain> iterator) {
         this.version = version;
         this.iterator = iterator;
         findNext();
      }

      @Override
      public boolean hasNext() {
         return next != null;
      }

      @Override
      public InternalCacheEntry next() {
         if (next == null) {
            throw new NoSuchElementException();
         }
         InternalCacheEntry toReturn = next;
         findNext();
         return toReturn;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      private void findNext() {
         next = null;
         while (iterator.hasNext()) {
            VersionChain chain = iterator.next();
            next = chain.get(version).getEntry();
            if (next != null) {
               return;
            }
         }
      }
   }

   private static class GMUVersionEntry {
      private final InternalCacheEntry entry;
      private final boolean mostRecent;

      private GMUVersionEntry(InternalCacheEntry entry, boolean mostRecent) {
         this.entry = entry;
         this.mostRecent = mostRecent;
      }

      public InternalCacheEntry getEntry() {
         return entry;
      }

      public boolean isMostRecent() {
         return mostRecent;
      }
   }

   public static class VersionChain {
      private VersionBody first;

      public final GMUVersionEntry get(EntryVersion version) {
         VersionBody iterator;
         synchronized (this) {
            iterator = first;
         }

         if (version == null) {
            InternalCacheEntry entry = iterator == null ? null : iterator.getInternalCacheEntry();
            return new GMUVersionEntry(entry, true);
         }

         boolean mostRecent = true;

         while (iterator != null) {
            if (isOlderOrEqual(iterator, version)) {
               return new GMUVersionEntry(iterator.getInternalCacheEntry(), mostRecent);
            }
            iterator = iterator.getPrevious();
            mostRecent = false;
         }
         return new GMUVersionEntry(null, mostRecent);
      }

      public final VersionBody add(InternalCacheEntry cacheEntry) {
         VersionBody toAdd = new VersionBody(cacheEntry);
         VersionBody iterator = firstAdd(toAdd);
         while (iterator != null) {
            iterator = iterator.add(toAdd);
         }
         return toAdd.getPrevious();
      }

      public final boolean contains(EntryVersion version) {
         VersionBody iterator;
         synchronized (this) {
            iterator = first;
         }

         if (version == null) {
            return iterator != null && !iterator.getInternalCacheEntry().isNull();
         }

         while (iterator != null) {
            if (isOlderOrEqual(iterator, version)) {
               return !iterator.getInternalCacheEntry().isNull();
            }
            iterator = iterator.getPrevious();
         }
         return false;
      }

      public final GMUVersionEntry remove(Object key, EntryVersion version) {
         VersionBody previous = add(new InternalGMURemovedCacheEntry(key, version));
         InternalCacheEntry entry = previous == null ? null : previous.getInternalCacheEntry();
         return new GMUVersionEntry(entry, first == null || first.getPrevious() == previous);
      }

      public final void purgeExpired(long now) {
         VersionBody iterator;
         synchronized (this) {
            while (first != null && isExpired(first, now)) {
               first = first.getPrevious();
            }
            iterator = first;
         }
         while (iterator != null) {
            iterator = iterator.expire(now);
         }
      }

      private synchronized VersionBody firstAdd(VersionBody body) {
         if (first == null || isOlder(first, body)) {
            body.setPrevious(first);
            first = body;
            return null;
         }
         return first.add(body);
      }
   }

   public static class VersionBody {
      private final InternalCacheEntry internalCacheEntry;
      private VersionBody previous;

      public VersionBody(InternalCacheEntry internalCacheEntry) {
         this.internalCacheEntry = internalCacheEntry;
      }

      public InternalCacheEntry getInternalCacheEntry() {
         return internalCacheEntry;
      }

      public synchronized VersionBody getPrevious() {
         return previous;
      }

      private synchronized void setPrevious(VersionBody previous) {
         this.previous = previous;
      }

      public synchronized VersionBody add(VersionBody other) {
         if (previous == null) {
            previous = other;
            return null;
         } else if (isOlder(previous, other)) {
            other.setPrevious(previous);
            setPrevious(other);
            return null;
         }
         return previous;
      }

      public synchronized VersionBody expire(long now) {
         if (previous == null) {
            return null;
         }
         if (isExpired(previous, now)) {
            previous = previous.getPrevious();
            return this;
         }
         return previous;
      }
   }
}
