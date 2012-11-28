package org.infinispan.container.versioning.gmu;

import org.infinispan.Cache;
import org.infinispan.cacheviews.CacheView;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import static org.infinispan.container.versioning.gmu.GMUEntryVersion.NON_EXISTING;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUEntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistGMUVersionGenerator implements GMUVersionGenerator {

   private static final Hash HASH = new MurmurHash3();
   private static final Log log = LogFactory.getLog(DistGMUVersionGenerator.class);

   private RpcManager rpcManager;
   private String cacheName;
   private final TreeMap<Integer, ClusterSnapshot> viewIdClusterSnapshot;
   private volatile int currentViewId;

   public DistGMUVersionGenerator() {
      viewIdClusterSnapshot = new TreeMap<Integer, ClusterSnapshot>();
   }

   @Inject
   public final void init(RpcManager rpcManager, Cache cache) {
      this.rpcManager = rpcManager;
      this.cacheName = cache.getName();
   }

   @Start(priority = 11) // needs to happen *after* the transport starts.
   public final void setEmptyViewId() {
      currentViewId = -1;
      viewIdClusterSnapshot.put(-1, new ClusterSnapshot(Collections.singleton(rpcManager.getAddress()), HASH));
   }

   @Override
   public final IncrementableEntryVersion generateNew() {
      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versions = create(true, clusterSnapshot.size());
      versions[clusterSnapshot.indexOf(getAddress())] = 0;
      return new GMUClusterEntryVersion(cacheName, currentViewId, this, versions);
   }

   @Override
   public final IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      if (initialVersion == null) {
         throw new NullPointerException("Cannot increment a null version");
      }

      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(initialVersion);
      int viewId = currentViewId;
      GMUClusterEntryVersion incrementedVersion = new GMUClusterEntryVersion(cacheName, viewId, this,
                                                                             increment(gmuEntryVersion, viewId));

      if (log.isTraceEnabled()) {
         log.tracef("increment(%s) ==> %s", initialVersion, incrementedVersion);
      }
      return incrementedVersion;
   }

   @Override
   public final GMUEntryVersion mergeAndMax(Collection<? extends EntryVersion> entryVersions) {
      if (entryVersions == null || entryVersions.isEmpty()) {
         throw new IllegalStateException("Cannot merge an empy list");
      }

      List<GMUEntryVersion> gmuEntryVersions = new ArrayList<GMUEntryVersion>(entryVersions.size());
      //validate the entry versions
      for (EntryVersion entryVersion : entryVersions) {
         if (entryVersion == null) {
            log.errorf("Null version in list %s. It will be ignored", entryVersion);
         } else if (entryVersion instanceof GMUEntryVersion) {
            gmuEntryVersions.add(toGMUEntryVersion(entryVersion));
         } else {
            throw new IllegalArgumentException("Expected an array of GMU entry version but it has " +
                                                     entryVersion.getClass().getSimpleName());
         }
      }

      int viewId = currentViewId;
      GMUClusterEntryVersion mergedVersion = new GMUClusterEntryVersion(cacheName, viewId, this,
                                                                        mergeClustered(viewId, gmuEntryVersions));
      if (log.isTraceEnabled()) {
         log.tracef("mergeAndMax(%s) ==> %s", entryVersions, mergedVersion);
      }
      return mergedVersion;
   }

   @Override
   public final GMUEntryVersion calculateCommitVersion(EntryVersion prepareVersion,
                                                       Collection<Address> affectedOwners) {
      int viewId = currentViewId;
      GMUClusterEntryVersion commitVersion = new GMUClusterEntryVersion(cacheName, viewId, this,
                                                                        calculateVersionToCommit(viewId, prepareVersion,
                                                                                                 affectedOwners));
      if (log.isTraceEnabled()) {
         log.tracef("calculateCommitVersion(%s,%s) ==> %s", prepareVersion, affectedOwners, commitVersion);
      }
      return commitVersion;
   }

   @Override
   public final GMUEntryVersion convertVersionToWrite(EntryVersion version) {
      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(version);
      GMUCacheEntryVersion writeVersion = new GMUCacheEntryVersion(cacheName, currentViewId, this,
                                                                   gmuEntryVersion.getThisNodeVersionValue());

      if (log.isTraceEnabled()) {
         log.tracef("convertVersionToWrite(%s) ==> %s", version, writeVersion);
      }
      return writeVersion;
   }

   @Override
   public GMUEntryVersion calculateMaxVersionToRead(EntryVersion transactionVersion,
                                                    Collection<Address> alreadyReadFrom) {
      if (alreadyReadFrom == null || alreadyReadFrom.isEmpty()) {
         if (log.isTraceEnabled()) {
            log.tracef("calculateMaxVersionToRead(%s, %s) ==> null", transactionVersion, alreadyReadFrom);
         }
         return null;
      }
      //the max version is calculated with the position of the version in which this node has already read from
      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(transactionVersion);

      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versionsValues = create(true, clusterSnapshot.size());

      for (Address readFrom : alreadyReadFrom) {
         int index = clusterSnapshot.indexOf(readFrom);
         if (index == -1) {
            //does not exists in current view (is this safe? -- I think that it depends of the state transfer...)
            continue;
         }
         versionsValues[index] = gmuEntryVersion.getVersionValue(readFrom);
      }

      GMUClusterEntryVersion maxVersionToRead = new GMUClusterEntryVersion(cacheName, viewId, this, versionsValues);
      if (log.isTraceEnabled()) {
         log.tracef("calculateMaxVersionToRead(%s, %s) ==> %s", transactionVersion, alreadyReadFrom, maxVersionToRead);
      }
      return maxVersionToRead;
   }

   @Override
   public GMUEntryVersion calculateMinVersionToRead(EntryVersion transactionVersion,
                                                    Collection<Address> alreadyReadFrom) {
      if (transactionVersion == null) {
         throw new NullPointerException("Transaction Version cannot be null to calculate the min version to read");
      }
      if (alreadyReadFrom == null || alreadyReadFrom.isEmpty()) {
         if (log.isTraceEnabled()) {
            log.tracef("calculateMinVersionToRead(%s, %s) ==> %s", transactionVersion, alreadyReadFrom,
                       transactionVersion);
         }
         return toGMUEntryVersion(transactionVersion);
      }

      //the min version is defined by the nodes that we haven't read yet

      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(transactionVersion);
      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versionValues = create(true, clusterSnapshot.size());

      for (int i = 0; i < clusterSnapshot.size(); ++i) {
         Address address = clusterSnapshot.get(i);
         if (alreadyReadFrom.contains(address)) {
            continue;
         }
         versionValues[i] = gmuEntryVersion.getVersionValue(address);
      }

      GMUClusterEntryVersion minVersionToRead = new GMUClusterEntryVersion(cacheName, viewId, this, versionValues);
      if (log.isTraceEnabled()) {
         log.tracef("calculateMinVersionToRead(%s, %s) ==> %s", transactionVersion, alreadyReadFrom, minVersionToRead);
      }
      return minVersionToRead;
   }

   @Override
   public GMUEntryVersion setNodeVersion(EntryVersion version, long value) {

      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(version);
      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versionValues = create(true, clusterSnapshot.size());

      for (int i = 0; i < clusterSnapshot.size(); ++i) {
         Address address = clusterSnapshot.get(i);
         versionValues[i] = gmuEntryVersion.getVersionValue(address);
      }

      versionValues[clusterSnapshot.indexOf(getAddress())] = value;

      GMUClusterEntryVersion newVersion = new GMUClusterEntryVersion(cacheName, viewId, this, versionValues);
      if (log.isTraceEnabled()) {
         log.tracef("setNodeVersion(%s, %s) ==> %s", version, value, newVersion);
      }
      return newVersion;
   }

   @Override
   public synchronized final ClusterSnapshot getClusterSnapshot(int viewId) {
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

   @Override
   public synchronized void updateViewHistory(List<CacheView> viewHistory) {
      //this is a little overkill =(
      for (CacheView cacheView : viewHistory) {
         addCacheView(cacheView);
      }
   }

   @Override
   public synchronized void addCacheView(CacheView cacheView) {
      int viewId = cacheView.getViewId();
      if (viewIdClusterSnapshot.containsKey(cacheView.getViewId())) {
         //can this happen??
         if (log.isTraceEnabled()) {
            log.tracef("Update members to view Id %s. But it already exists.", viewId);
         }
         return;
      }

      Collection<Address> addresses = cacheView.getMembers();
      if (log.isTraceEnabled()) {
         log.tracef("Update members to view Id %s. Members are %s", viewId, addresses);
      }
      currentViewId = viewId;
      viewIdClusterSnapshot.put(viewId, new ClusterSnapshot(addresses, HASH));
      notifyAll();
   }

   private long[] create(boolean fill, int size) {
      long[] versions = new long[size];
      if (fill) {
         Arrays.fill(versions, NON_EXISTING);
      }
      return versions;
   }

   private long[] increment(GMUEntryVersion initialVersion, int viewId) {
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);

      long[] versions = create(false, clusterSnapshot.size());

      for (int index = 0; index < clusterSnapshot.size(); ++index) {
         versions[index] = initialVersion.getVersionValue(clusterSnapshot.get(index));
      }

      int myIndex = clusterSnapshot.indexOf(getAddress());
      versions[myIndex]++;
      return versions;
   }

   private long[] mergeClustered(int viewId, Collection<GMUEntryVersion> entryVersions) {
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versions = create(true, clusterSnapshot.size());

      for (GMUEntryVersion entryVersion : entryVersions) {
         for (int index = 0; index < clusterSnapshot.size(); ++index) {
            versions[index] = Math.max(versions[index], entryVersion.getVersionValue(clusterSnapshot.get(index)));
         }
      }
      return versions;
   }

   private long[] calculateVersionToCommit(int newViewId, EntryVersion version, Collection<Address> addresses) {
      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(version);

      if (addresses == null) {
         int oldViewId = gmuEntryVersion.getViewId();
         ClusterSnapshot oldClusterSnapshot = getClusterSnapshot(oldViewId);
         long commitValue = 0;
         for (int i = 0; i < oldClusterSnapshot.size(); ++i) {
            commitValue = Math.max(commitValue, gmuEntryVersion.getVersionValue(i));
         }

         ClusterSnapshot clusterSnapshot = getClusterSnapshot(newViewId);
         long[] versions = create(true, clusterSnapshot.size());
         for (int i = 0; i < clusterSnapshot.size(); ++i) {
            versions[i] = commitValue;
         }
         return versions;
      }

      ClusterSnapshot clusterSnapshot = getClusterSnapshot(newViewId);
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
