package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUClusterEntryVersion extends GMUEntryVersion {

   private final long[] versions;

   public GMUClusterEntryVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator, long[] versions) {
      super(cacheName, viewId, versionGenerator);
      if (versions.length != clusterSnapshot.size()) {
         throw new IllegalArgumentException("Version vector (size " + versions.length + ") has not the expected size " +
                                                  clusterSnapshot.size());
      }
      this.versions = Arrays.copyOf(versions, clusterSnapshot.size());
   }

   private GMUClusterEntryVersion(String cacheName, int viewId, ClusterSnapshot clusterSnapshot, Address localAddress,
                                  long[] versions) {
      super(cacheName, viewId, clusterSnapshot, localAddress);
      this.versions = versions;
   }

   @Override
   public long getVersionValue(Address address) {
      return getVersionValue(clusterSnapshot.indexOf(address));
   }

   @Override
   public long getVersionValue(int addressIndex) {
      return validIndex(addressIndex) ? versions[addressIndex] : NON_EXISTING;
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
         //is this safe?
         return InequalVersionComparisonResult.BEFORE_OR_EQUAL;

      }
      throw new IllegalArgumentException("GMU entry version cannot compare " + other.getClass().getSimpleName());
   }

   private boolean validIndex(int index) {
      return index >= 0 && index < versions.length;
   }

   @Override
   public String toString() {
      return "GMUClusterEntryVersion{" +
            "versions=" + versionsToString(versions, clusterSnapshot) +
            ", " + super.toString();
   }

   public static class Externalizer extends AbstractExternalizer<GMUClusterEntryVersion> {

      private final GlobalComponentRegistry globalComponentRegistry;

      public Externalizer(GlobalComponentRegistry globalComponentRegistry) {
         this.globalComponentRegistry = globalComponentRegistry;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Set<Class<? extends GMUClusterEntryVersion>> getTypeClasses() {
         return Util.<Class<? extends GMUClusterEntryVersion>>asSet(GMUClusterEntryVersion.class);
      }

      @Override
      public void writeObject(ObjectOutput output, GMUClusterEntryVersion object) throws IOException {
         output.writeUTF(object.cacheName);
         output.writeInt(object.viewId);
         for (long v : object.versions) {
            output.writeLong(v);
         }
      }

      @Override
      public GMUClusterEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = input.readUTF();
         GMUVersionGenerator gmuVersionGenerator = getGMUVersionGenerator(globalComponentRegistry, cacheName);
         int viewId = input.readInt();
         ClusterSnapshot clusterSnapshot = gmuVersionGenerator.getClusterSnapshot(viewId);
         if (clusterSnapshot == null) {
            throw new IllegalArgumentException("View Id " + viewId + " not found in this node");
         }
         long[] versions = new long[clusterSnapshot.size()];
         for (int i = 0; i < versions.length; ++i) {
            versions[i] = input.readLong();
         }
         return new GMUClusterEntryVersion(cacheName, viewId, clusterSnapshot, gmuVersionGenerator.getAddress(), versions);
      }

      @Override
      public Integer getId() {
         return Ids.GMU_CLUSTER_VERSION;
      }
   }
}