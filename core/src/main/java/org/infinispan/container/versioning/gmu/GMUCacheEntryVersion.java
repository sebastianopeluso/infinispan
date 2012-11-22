package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUCacheEntryVersion extends GMUEntryVersion {

   private static final Log log = LogFactory.getLog(GMUCacheEntryVersion.class);
   
   private final long version;

   public GMUCacheEntryVersion(long viewId, GMUVersionGenerator versionGenerator, long version) {
      super(viewId, versionGenerator);
      this.version = version;
   }

   @Override
   public long getVersionValue(Address address) {
      return getVersionValue(clusterSnapshot.indexOf(address));
   }

   @Override
   public long getVersionValue(int addressIndex) {
      return addressIndex == nodeIndex ? version : NON_EXISTING;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      if (other instanceof GMUCacheEntryVersion) {
         GMUCacheEntryVersion cacheEntryVersion = (GMUCacheEntryVersion) other;
         InequalVersionComparisonResult versionComparisonResult = compare(this.version, cacheEntryVersion.version);

         if (versionComparisonResult == InequalVersionComparisonResult.EQUAL) {
            versionComparisonResult = compare(this.viewId, cacheEntryVersion.viewId);
         }

         if (log.isTraceEnabled()) {
            log.tracef("Compare this[%s] with other[%s] => %s", this, other, versionComparisonResult);
         }
         return versionComparisonResult;
      }

      if (other instanceof GMUClusterEntryVersion) {
         GMUClusterEntryVersion clusterEntryVersion = (GMUClusterEntryVersion) other;
         InequalVersionComparisonResult versionComparisonResult = compare(this.version, clusterEntryVersion.getThisNodeVersionValue());

         if (versionComparisonResult == InequalVersionComparisonResult.EQUAL) {
            versionComparisonResult = compare(this.viewId, clusterEntryVersion.viewId);
         }

         if (log.isTraceEnabled()) {
            log.tracef("Compare this[%s] with other[%s] => %s", this, other, versionComparisonResult);
         }
         
         return versionComparisonResult;
      }

      throw new IllegalArgumentException("Cannot compare GMU entry versions with " + (other == null ? "N/A" :
                                                                                            other.getClass().getSimpleName()));
   }

   @Override
   public String toString() {
      return "GMUCacheEntryVersion{" +
            "version=" + version +
            ", " + super.toString();
   }
}
