package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUNullCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMURemovedCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUValueCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.gmu.GMUHelper;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;
import static org.infinispan.transaction.gmu.GMUHelper.toInternalGMUCacheEntry;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUDataContainer extends AbstractDataContainer<GMUDataContainer.VersionChain> {

   private static final Log log = LogFactory.getLog(GMUDataContainer.class);

   private GMUVersionGenerator gmuVersionGenerator;

   protected GMUDataContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   protected GMUDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy) {
      super(concurrencyLevel, maxEntries, strategy, policy);
   }

   @Inject
   public void setVersionGenerator(VersionGenerator versionGenerator) {
      this.gmuVersionGenerator = GMUHelper.toGMUVersionGenerator(versionGenerator);
   }

   @Override
   public InternalCacheEntry get(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s)", k, version);
      }
      InternalCacheEntry entry = peek(k, version);
      long now = System.currentTimeMillis();
      if (entry.canExpire() && entry.isExpired(now)) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.get(%s,%s) => EXPIRED", k, version);
         }

         return new InternalGMUNullCacheEntry(toInternalGMUCacheEntry(entry));
      }
      entry.touch(now);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s) => %s", k, version, entry);
      }

      return entry;
   }

   @Override
   public InternalCacheEntry peek(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s)", k, version);
      }

      VersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.peek(%s,%s) => NOT_FOUND", k, version);
         }
         return wrap(k, null, true, version);
      }
      GMUVersionEntry entry = chain.get(version);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s) => %s", k, version, entry);
      }
      return wrap(k, entry.getEntry(), entry.isMostRecent(), version);
   }

   @Override
   public void put(Object k, Object v, EntryVersion version, long lifespan, long maxIdle) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s,%s,%s)", k, v, version, lifespan, maxIdle);
      }
      VersionChain chain = entries.get(k);

      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.put(%s,%s,%s,%s,%s), create new VersionChain", k, v, version, lifespan, maxIdle);
         }
         chain = new VersionChain();
         entries.put(k, chain);
      }

      EntryVersion maxVersion = getVersionToWrite(version);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s,%s,%s), correct version is %s", k, v, version, lifespan, maxIdle, maxVersion);
      }

      chain.add(entryFactory.create(k, v, maxVersion, lifespan, maxIdle));
      if (log.isTraceEnabled()) {
         StringBuilder stringBuilder = new StringBuilder();
         chain.chainToString(stringBuilder);
         log.tracef("Updated chain is %s", stringBuilder);
      }
   }

   @Override
   public boolean containsKey(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.containsKey(%s,%s)", k, version);
      }

      VersionChain chain = entries.get(k);
      boolean contains = chain != null && chain.contains(version);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.containsKey(%s,%s) => %s", k, version, contains);
      }

      return contains;
   }

   @Override
   public InternalCacheEntry remove(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.remove(%s,%s)", k, version);
      }

      VersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.remove(%s,%s) => NOT_FOUND", k, version);
         }
         return wrap(k, null, true, null);
      }
      GMUVersionEntry entry = chain.remove(k, getVersionToWrite(version));

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.remove(%s,%s) => %s", k, version, entry);
      }
      return wrap(k, entry.getEntry(), entry.isMostRecent(), null);
   }

   @Override
   public int size(EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.size(%s)", version);
      }
      int size = 0;
      for (VersionChain chain : entries.values()) {
         if (chain.contains(version)) {
            size++;
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.size(%s) => %s", version, size);
      }
      return size;
   }

   @Override
   public void clear() {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.clear()");
      }
      entries.clear();
   }

   @Override
   public void clear(EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.clear(%s)", version);
      }
      for (Object key : entries.keySet()) {
         remove(key, version);
      }
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = System.currentTimeMillis();
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.purgeExpired(%s)", currentTimeMillis);
      }
      for (VersionChain chain : entries.values()) {
         chain.purgeExpired(currentTimeMillis);
      }
   }

   @Override
   public final boolean dumpTo(String filePath) {
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         for (Map.Entry<Object, VersionChain> entry : entries.entrySet()) {
            Util.safeWrite(bufferedWriter, entry.getKey());
            Util.safeWrite(bufferedWriter, "=");
            entry.getValue().dumpChain(bufferedWriter);
            bufferedWriter.newLine();
            bufferedWriter.flush();
         }
         return true;
      } catch (IOException e) {
         return false;
      } finally {
         Util.close(bufferedWriter);
      }
   }

   public final String stateToString() {
      StringBuilder stringBuilder = new StringBuilder(8132);
      for (Map.Entry<Object, VersionChain> entry : entries.entrySet()) {
         stringBuilder.append(entry.getKey())
               .append("=");
         entry.getValue().chainToString(stringBuilder);
         stringBuilder.append("\n");
      }
      return stringBuilder.toString();
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
      return entry == null ? null : entry.get(version).getEntry();
   }

   @Override
   protected EntryIterator createEntryIterator(EntryVersion version) {
      return new GMUEntryIterator(version, entries.values().iterator());
   }

   private EntryVersion getVersionToWrite(EntryVersion version) {
      if (version == null) {
         throw new NullPointerException("Null version are not allowed to set to entry value");
      }
      return gmuVersionGenerator.convertVersionToWrite(version);
   }

   private static boolean isOlder(VersionBody current, VersionBody version) {
      return current.getInternalCacheEntry().getVersion().compareTo(version.getInternalCacheEntry().getVersion()) == BEFORE;
   }

   private static boolean isOlderOrEqual(VersionBody current, EntryVersion version) {
      InequalVersionComparisonResult result = current.getInternalCacheEntry().getVersion().compareTo(version);
      return result == BEFORE || result == BEFORE_OR_EQUAL || result == EQUAL;
   }

   private static boolean isExpired(VersionBody body, long now) {
      InternalCacheEntry entry = body.getInternalCacheEntry();
      return entry != null && entry.canExpire() && entry.isExpired(now);
   }

   private static InternalCacheEntry wrap(Object key, InternalCacheEntry entry, boolean mostRecent,
                                          EntryVersion maxTxVersion) {
      if (entry == null || entry.isNull()) {
         return new InternalGMUNullCacheEntry(key, (entry == null ? null : entry.getVersion()), maxTxVersion, mostRecent, null, null
         );
      }
      return new InternalGMUValueCacheEntry(entry, maxTxVersion, mostRecent, null, null);
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

      @Override
      public String toString() {
         return "GMUVersionEntry{" +
               "entry=" + entry +
               ", mostRecent=" + mostRecent +
               '}';
      }
   }

   public static class VersionChain {
      private VersionBody first;

      public final GMUVersionEntry get(EntryVersion version) {
         VersionBody iterator;
         synchronized (this) {
            iterator = first;
         }

         if (log.isTraceEnabled()) {
            log.tracef("[%s] find value for version %s", Thread.currentThread().getName(), version);
         }

         if (version == null) {
            InternalCacheEntry entry = iterator == null ? null : iterator.getInternalCacheEntry();
            if (log.isTraceEnabled()) {
               log.tracef("[%s] version is null... returning the most recent: %s", Thread.currentThread().getName(), entry);
            }
            return new GMUVersionEntry(entry, true);
         }

         boolean mostRecent = true;

         while (iterator != null) {
            if (isOlderOrEqual(iterator, version)) {
               if (log.isTraceEnabled()) {
                  log.tracef("[%s] value found: %s", Thread.currentThread().getName(), iterator);
               }
               return new GMUVersionEntry(iterator.getInternalCacheEntry(), mostRecent);
            }
            iterator = iterator.getPrevious();
            mostRecent = false;
         }

         if (log.isTraceEnabled()) {
            log.tracef("[%s] No value found!", Thread.currentThread().getName());
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

      public final String chainToString(StringBuilder stringBuilder) {
         VersionBody iterator;
         synchronized (this) {
            iterator = first;
         }
         while (iterator != null) {
            InternalCacheEntry entry = iterator.getInternalCacheEntry();
            stringBuilder.append(entry.getValue())
                  .append("=")
                  .append(entry.getVersion())
                  .append("|");
            iterator = iterator.getPrevious();
         }
         return stringBuilder.toString();
      }

      public final void dumpChain(BufferedWriter writer) throws IOException {
         VersionBody iterator;
         synchronized (this) {
            iterator = first;
         }
         while (iterator != null) {
            InternalCacheEntry entry = iterator.getInternalCacheEntry();
            Util.safeWrite(writer, entry.getValue());
            Util.safeWrite(writer, "=");
            Util.safeWrite(writer, entry.getVersion());
            Util.safeWrite(writer, "|");
            iterator = iterator.getPrevious();
         }
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

      @Override
      public String toString() {
         return "VersionBody{" +
               "internalCacheEntry=" + internalCacheEntry +
               '}';
      }
   }
}
