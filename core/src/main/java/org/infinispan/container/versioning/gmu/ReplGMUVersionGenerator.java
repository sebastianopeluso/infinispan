package org.infinispan.container.versioning.gmu;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

import static org.infinispan.container.versioning.gmu.GMUEntryVersion.NON_EXISTING;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Listener
public class ReplGMUVersionGenerator implements GMUVersionGenerator {

   private static final Hash HASH = new MurmurHash3();
   private RpcManager rpcManager;
   private ClusterSnapshot currentClusterSnapshot;
   private long currentViewId;

   public ReplGMUVersionGenerator() {}

   @Inject
   public final void init(RpcManager rpcManager, CacheManagerNotifier cacheManagerNotifier) {
      this.rpcManager = rpcManager;
      cacheManagerNotifier.addListener(this);
   }

   @Start(priority = 11)
   public final void start() {
      updateMembers(rpcManager.getTransport().getMembers(), rpcManager.getTransport().getViewId());
   }

   @Override
   public final IncrementableEntryVersion generateNew() {
      return new GMUCacheEntryVersion(currentViewId, this, 0);
   }

   @Override
   public final IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      GMUCacheEntryVersion gmuEntryVersion = toGMUEntryVersion(initialVersion);
      return new GMUCacheEntryVersion(currentViewId, this, gmuEntryVersion.getThisNodeVersionValue() + 1);
   }

   @Override
   public final IncrementableEntryVersion mergeAndMax(Collection<? extends EntryVersion> entryVersions) {
      //validate the entry versions      
      for (EntryVersion entryVersion : entryVersions) {
         if (entryVersion instanceof GMUCacheEntryVersion) {
            continue;
         }
         throw new IllegalArgumentException("Expected an array of GMU entry version but it has " +
                                                  entryVersion.getClass().getSimpleName());
      }

      return new GMUCacheEntryVersion(currentViewId, this, merge(entryVersions));
   }

   @Override
   public final IncrementableEntryVersion calculateCommitVersion(EntryVersion mergedPrepareVersion,
                                                                 Collection<Address> affectedOwners) {
      return (IncrementableEntryVersion) mergedPrepareVersion;
   }

   @Override
   public final IncrementableEntryVersion convertVersionToWrite(EntryVersion version) {
      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(version);
      return new GMUCacheEntryVersion(currentViewId, this, gmuEntryVersion.getThisNodeVersionValue());
   }

   @Override
   public IncrementableEntryVersion calculateMaxVersionToRead(EntryVersion transactionVersion,
                                                              Collection<Address> alreadyReadFrom) {
      return (IncrementableEntryVersion) transactionVersion;
   }

   @Override
   public IncrementableEntryVersion calculateMinVersionToRead(EntryVersion transactionVersion,
                                                              Collection<Address> alreadyReadFrom) {
      return (IncrementableEntryVersion) transactionVersion;
   }

   @Override
   public synchronized final ClusterSnapshot getClusterSnapshot(long viewId) {
      return currentClusterSnapshot;
   }

   @Override
   public final Address getAddress() {
      return rpcManager.getAddress();
   }

   @ViewChanged
   @Merged
   public final void handle(ViewChangedEvent event) {
      updateMembers(event.getNewMembers(), event.getViewId());
   }

   private synchronized void updateMembers(Collection<Address> addresses, long viewId) {
      currentViewId = viewId;
      currentClusterSnapshot = new ClusterSnapshot(addresses, HASH);
      notifyAll();
   }

   private GMUCacheEntryVersion toGMUEntryVersion(EntryVersion version) {
      if (version instanceof GMUCacheEntryVersion) {
         return (GMUCacheEntryVersion) version;
      }
      throw new IllegalArgumentException("Expected a GMU entry version but received " + version.getClass().getSimpleName());
   }

   private long merge(Collection<? extends EntryVersion> entryVersions) {
      long max = NON_EXISTING;
      for (EntryVersion entryVersion : entryVersions) {
         GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(entryVersion);
         max = Math.max(max, gmuEntryVersion.getThisNodeVersionValue());
      }
      return max;
   }
}
