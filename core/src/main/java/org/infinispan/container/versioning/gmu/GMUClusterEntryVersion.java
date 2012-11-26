package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.remoting.transport.Address;

import java.util.Arrays;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUClusterEntryVersion extends GMUEntryVersion {

   private final long[] versions;

   public GMUClusterEntryVersion(long viewId, GMUVersionGenerator versionGenerator) {
      super(viewId, versionGenerator);
      versions = new long[clusterSnapshot.size()];
      Arrays.fill(versions, NON_EXISTING);
   }

   public GMUClusterEntryVersion(long viewId, GMUVersionGenerator versionGenerator, long[] versions) {
      super(viewId, versionGenerator);
      if (versions.length != clusterSnapshot.size()) {
         throw new IllegalArgumentException("Version vector (size " + versions.length + ") has not the expected size " +
                                                  clusterSnapshot.size());
      }
      this.versions = Arrays.copyOf(versions, clusterSnapshot.size());
   }

   @Override
   public long getVersionValue(Address address) {
      return getVersionValue(clusterSnapshot.indexOf(address));
   }

   @Override
   public long getVersionValue(int addressIndex) {
      return validIndex(addressIndex) ? NON_EXISTING : versions[addressIndex];
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      if (other instanceof GMUCacheEntryVersion) {
         GMUCacheEntryVersion cacheEntryVersion = (GMUCacheEntryVersion) other;
         InequalVersionComparisonResult versionComparisonResult = compare(getThisNodeVersionValue(),
                                                                          cacheEntryVersion.getThisNodeVersionValue());

         if (versionComparisonResult == InequalVersionComparisonResult.EQUAL) {
            return compare(this.viewId, cacheEntryVersion.viewId);
         }

         return versionComparisonResult;
      }

      if (other instanceof GMUClusterEntryVersion) {
         GMUClusterEntryVersion clusterEntryVersion = (GMUClusterEntryVersion) other;

         boolean before = false, equal = false, after = false;

         for (int index = 0; index < clusterSnapshot.size(); ++index) {
            long myVersion = getVersionValue(index);
            long otherVersion = clusterEntryVersion.getVersionValue(clusterSnapshot.get(index));

            if (myVersion == NON_EXISTING || otherVersion == NON_EXISTING) {
               continue;
            }
            switch (compare(myVersion, otherVersion)) {
               case BEFORE:
                  before = true;
                  break;
               case EQUAL:
                  equal = true;
                  break;
               case AFTER:
                  after = true;
                  break;
            }
            if (before && after) {
               return InequalVersionComparisonResult.CONFLICTING;
            }
         }
         if (equal && after) {
            return InequalVersionComparisonResult.AFTER_OR_EQUAL;
         } else if (equal && before) {
            return InequalVersionComparisonResult.BEFORE_OR_EQUAL;
         } else if (equal) {
            return InequalVersionComparisonResult.EQUAL;
         } else if (before) {
            return InequalVersionComparisonResult.BEFORE;
         } else if (after) {
            return InequalVersionComparisonResult.AFTER;
         }
         throw new IllegalArgumentException("GMU entry version has nothing in common with this version. Cannot compare it");
      }
      throw new IllegalArgumentException("GMU entry version cannot compare " + other.getClass().getSimpleName());
   }

   private boolean validIndex(int index) {
      return index >= 0 && index < versions.length;
   }
}
