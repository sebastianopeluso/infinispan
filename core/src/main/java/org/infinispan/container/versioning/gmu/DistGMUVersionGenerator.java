package org.infinispan.container.versioning.gmu;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import static org.infinispan.container.versioning.gmu.GMUEntryVersion.NON_EXISTING;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Listener
public class DistGMUVersionGenerator implements GMUVersionGenerator {

   private static final Hash HASH = new MurmurHash3();
   private RpcManager rpcManager;
   private final TreeMap<Long, ClusterSnapshot> viewIdClusterSnapshot;
   private long currentViewId;

   public DistGMUVersionGenerator() {
      viewIdClusterSnapshot = new TreeMap<Long, ClusterSnapshot>();
   }

   @Inject
   public final void init(RpcManager rpcManager, CacheNotifier cacheNotifier) {
      this.rpcManager = rpcManager;
      cacheNotifier.addListener(this);
   }

   @Start(priority = 11)
   public final void start() {
      updateMembers(rpcManager.getTransport().getMembers(), rpcManager.getTransport().getViewId());
   }

   @Override
   public final IncrementableEntryVersion generateNew() {
      return new GMUClusterEntryVersion(currentViewId, this);
   }

   @Override
   public final IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(initialVersion);

      if (initialVersion instanceof GMUCacheEntryVersion) {
         return new GMUCacheEntryVersion(currentViewId, this, gmuEntryVersion.getThisNodeVersionValue() + 1);
      }

      long viewId = currentViewId;
      return new GMUClusterEntryVersion(viewId, this, increment(gmuEntryVersion, viewId));
   }

   @Override
   public final IncrementableEntryVersion mergeAndMax(Collection<? extends EntryVersion> entryVersions) {
      //validate the entry versions
      boolean clusterVersion = false;
      for (EntryVersion entryVersion : entryVersions) {
         if (entryVersion instanceof GMUClusterEntryVersion) {
            clusterVersion = true;
            continue;
         } else if (entryVersion instanceof GMUEntryVersion) {
            continue;
         }
         throw new IllegalArgumentException("Expected an array of GMU entry version but it has " +
                                                  entryVersion.getClass().getSimpleName());
      }

      if (clusterVersion) {
         long viewId = currentViewId;
         return new GMUClusterEntryVersion(viewId, this, mergeClustered(viewId, entryVersions));
      } else {
         return new GMUCacheEntryVersion(currentViewId, this, mergeNonClustered(entryVersions));
      }
   }

   @Override
   public final IncrementableEntryVersion calculateCommitVersion(EntryVersion prepareVersion,
                                                                 Collection<Address> affectedOwners) {
      if (prepareVersion instanceof GMUClusterEntryVersion) {
         long viewId = currentViewId;
         return new GMUClusterEntryVersion(viewId, this, calculateVersionToCommit(viewId, prepareVersion, affectedOwners));
      }
      return (IncrementableEntryVersion) prepareVersion;
   }

   @Override
   public final IncrementableEntryVersion convertVersionToWrite(EntryVersion version) {
      if (version instanceof GMUEntryVersion) {
         return new GMUCacheEntryVersion(currentViewId, this, ((GMUEntryVersion) version).getThisNodeVersionValue());
      }
      return (IncrementableEntryVersion) version;
   }

   @Override
   public IncrementableEntryVersion calculateMaxVersionToRead(EntryVersion transactionVersion,
                                                              Collection<Address> alreadyReadFrom) {
      return null; // TODO: Customise this generated block
   }

   @Override
   public IncrementableEntryVersion calculateMinVersionToRead(EntryVersion transactionVersion,
                                                              Collection<Address> alreadyReadFrom) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public synchronized final ClusterSnapshot getClusterSnapshot(long viewId) {
      if(viewId < viewIdClusterSnapshot.firstKey()) {
         //we don't have this view id anymore
         return null;
      }
      while (!viewIdClusterSnapshot.containsKey(viewId)) {
         try {
            wait();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      return viewIdClusterSnapshot.get(viewId);
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

   private long[] create(boolean fill, int size) {
      long[] versions = new long[size];
      if (fill) {
         Arrays.fill(versions, NON_EXISTING);
      }
      return versions;
   }

   private synchronized void updateMembers(Collection<Address> addresses, long viewId) {
      if (viewIdClusterSnapshot.containsKey(viewId)) {
         //can this happen??
         return;
      }
      currentViewId = viewId;
      viewIdClusterSnapshot.put(viewId, new ClusterSnapshot(addresses, HASH));
      notifyAll();
   }

   private GMUEntryVersion toGMUEntryVersion(EntryVersion version) {
      if (version instanceof GMUEntryVersion) {
         return (GMUEntryVersion) version;
      }
      throw new IllegalArgumentException("Expected a GMU entry version but received " + version.getClass().getSimpleName());
   }

   private long[] increment(GMUEntryVersion initialVersion, long viewId) {
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);

      long[] versions = create(false, clusterSnapshot.size());

      for (int index = 0; index < clusterSnapshot.size(); ++index) {
         versions[index] = initialVersion.getVersionValue(clusterSnapshot.get(index));
      }

      int myIndex = clusterSnapshot.indexOf(getAddress());
      versions[myIndex]++;
      return versions;
   }

   private long mergeNonClustered(Collection<? extends EntryVersion> entryVersions) {
      long max = NON_EXISTING;
      for (EntryVersion entryVersion : entryVersions) {
         GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(entryVersion);
         max = Math.max(max, gmuEntryVersion.getThisNodeVersionValue());
      }
      return max;
   }

   private long[] mergeClustered(long viewId, Collection< ? extends EntryVersion> entryVersions) {
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versions = create(true, clusterSnapshot.size());

      for (EntryVersion entryVersion : entryVersions) {
         GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(entryVersion);
         for (int index = 0; index < clusterSnapshot.size(); ++index) {
            versions[index] = Math.max(versions[index], gmuEntryVersion.getVersionValue(clusterSnapshot.get(index)));
         }
      }
      return versions;
   }

   private long[] calculateVersionToCommit(long viewId, EntryVersion version, Collection<Address> addresses) {
      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(version);
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versions = create(true, clusterSnapshot.size());
      List<Integer> ownersIndex = new LinkedList<Integer>();
      long commitValue = 0;

      for(Address owner : addresses) {
         int index = clusterSnapshot.indexOf(owner);
         if (index < 0) {
            continue;
         }
         commitValue = Math.max(commitValue, gmuEntryVersion.getVersionValue(owner));
         ownersIndex.add(index);
      }

      for (int index = 0; index < clusterSnapshot.size(); ++index) {
         if (ownersIndex.contains(index)) {
            versions[index] = commitValue;
         } else {
            versions[index] = gmuEntryVersion.getVersionValue(clusterSnapshot.get(index));
         }
      }
      return versions;
   }
}
