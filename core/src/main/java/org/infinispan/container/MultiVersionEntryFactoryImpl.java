package org.infinispan.container;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullMarkerEntry;
import org.infinispan.container.entries.NullMarkerEntryForRemoval;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.container.entries.StateChangingEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class MultiVersionEntryFactoryImpl extends EntryFactoryImpl {

   private VersionVCFactory versionVCFactory;

   private static final Log log = LogFactory.getLog(MultiVersionEntryFactoryImpl.class);

   @Inject
   public void injectDependencies(VersionVCFactory versionVCFactory) {
      this.versionVCFactory = versionVCFactory;
   }

   @Override
   protected MVCCEntry createWrappedEntry(Object key, Object value, EntryVersion version, boolean isForInsert, boolean forRemoval, long lifespan) {
      if (value == null && !isForInsert) {
         return forRemoval ? new NullMarkerEntryForRemoval(key, version) : NullMarkerEntry.getInstance();
      }

      return new SerializableEntry(key, value, lifespan, version);
   }

   @Override
   public final CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      boolean trace = log.isTraceEnabled();
      ctx.clearLastReadKey();//Clear last read key field;

      CacheEntry cacheEntry = getFromContext(ctx, key);
      //Is this key already written? (We don't have in lookup table a key previously read)
      if (cacheEntry == null) { //NO
         if (ctx.isInTxScope() || ctx.readBasedOnVersion()) {                        
            VersionVC maxToRead = ctx.calculateVersionToRead(this.versionVCFactory);                       
            boolean hasAlreadyReadFromThisNode = ctx.getAlreadyReadOnNode();

            if (trace) {
               log.tracef("Wrapping entry for read based on version. Key is %s, maximum version to read is %s and has " +
                                "already read from this node? %s", key, maxToRead, hasAlreadyReadFromThisNode);
            }

            InternalMVCCEntry ime = container.get(key, maxToRead, !hasAlreadyReadFromThisNode);
            cacheEntry = ime.getValue();

            MVCCEntry mvccEntry;
            if (cacheEntry == null) {
               mvccEntry = createWrappedEntry(key, null, null, false, false, -1);
            } else {
               mvccEntry = createWrappedEntry(key, cacheEntry.getValue(), cacheEntry.getVersion(), false, false, cacheEntry.getLifespan());
               // If the original entry has changeable state, copy state flags to the new MVCC entry.
               if (cacheEntry instanceof StateChangingEntry && mvccEntry != null)
                  mvccEntry.copyStateFlagsFrom((StateChangingEntry) cacheEntry);
            }

            if (trace) {
               log.tracef("[key=%s] Multiversion entry found is %s. Wrapped entry is %s", key, ime, mvccEntry);
            }
            if (mvccEntry != null) {
               ctx.addLocalReadKey(key,ime); //Add to the readSet
               ctx.setLastReadKey(mvccEntry); //Remember the last read key               
            }

            return mvccEntry;
         } else {            
            cacheEntry = getFromContainer(key);
            if (trace) {
               log.tracef("Wrapping entry for read ignoring versions. Key is %s and cache entry is %s", key, cacheEntry);
            }
            if(cacheEntry != null) {
               ctx.setLastReadKey(cacheEntry);               
            }
            return cacheEntry;
         }
      } else {//Yes
         if (trace) {
            log.tracef("[key=%s] is already in context", key);
         }
         ctx.setLastReadKey(cacheEntry);
         return cacheEntry;
      }
   }
}
