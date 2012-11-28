package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
public class GMUCacheEntryVersion extends GMUEntryVersion {

   private static final Log log = LogFactory.getLog(GMUCacheEntryVersion.class);

   private final long version;

   public GMUCacheEntryVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator, long version) {
      super(cacheName, viewId, versionGenerator);
      this.version = version;
   }

   private GMUCacheEntryVersion(String cacheName, int viewId, ClusterSnapshot clusterSnapshot, Address localAddress, long version) {
      super(cacheName, viewId, clusterSnapshot, localAddress);
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

   public static class Externalizer extends AbstractExternalizer<GMUCacheEntryVersion> {

      private final GlobalComponentRegistry globalComponentRegistry;

      public Externalizer(GlobalComponentRegistry globalComponentRegistry) {
         this.globalComponentRegistry = globalComponentRegistry;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Set<Class<? extends GMUCacheEntryVersion>> getTypeClasses() {
         return Util.<Class<? extends GMUCacheEntryVersion>>asSet(GMUCacheEntryVersion.class);
      }

      @Override
      public void writeObject(ObjectOutput output, GMUCacheEntryVersion object) throws IOException {
         output.writeUTF(object.cacheName);
         output.writeInt(object.viewId);
         output.writeLong(object.version);
      }

      @Override
      public GMUCacheEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = input.readUTF();
         GMUVersionGenerator gmuVersionGenerator = getGMUVersionGenerator(globalComponentRegistry, cacheName);
         int viewId = input.readInt();
         ClusterSnapshot clusterSnapshot = gmuVersionGenerator.getClusterSnapshot(viewId);
         if (clusterSnapshot == null) {
            throw new IllegalArgumentException("View Id " + viewId + " not found in this node");
         }
         long version = input.readLong();
         return new GMUCacheEntryVersion(cacheName, viewId, clusterSnapshot, gmuVersionGenerator.getAddress(), version);
      }

      @Override
      public Integer getId() {
         return Ids.GMU_ENTRY_VERSION;
      }
   }
}
