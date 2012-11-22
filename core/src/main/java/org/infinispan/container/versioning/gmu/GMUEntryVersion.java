package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.remoting.transport.Address;

import java.io.Serializable;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class GMUEntryVersion implements IncrementableEntryVersion, Serializable {

   public static final long NON_EXISTING = -1;
   
   protected final long viewId;
   protected transient ClusterSnapshot clusterSnapshot;
   protected transient int nodeIndex;

   protected GMUEntryVersion(long viewId, GMUVersionGenerator versionGenerator) {
      this.viewId = viewId;
      init(versionGenerator);
   }

   public final void init(GMUVersionGenerator versionGenerator) {
      clusterSnapshot = versionGenerator.getClusterSnapshot(viewId);
      nodeIndex = clusterSnapshot.indexOf(versionGenerator.getAddress());
      checkState();
   }

   public final long getViewId() {
      return viewId;
   }

   public abstract long getVersionValue(Address address);

   public abstract long getVersionValue(int addressIndex);
   
   public final long getThisNodeVersionValue() {
      return getVersionValue(nodeIndex);            
   }
   
   protected final void checkState() {
      if (clusterSnapshot == null) {
         throw new IllegalStateException("Cluster Snapshot in GMU entry version cannot be null");
      } else if (nodeIndex == NON_EXISTING) {
         throw new IllegalStateException("This node index in GMU entry version cannot be null");
      }
   }

   protected final InequalVersionComparisonResult compare(long value1, long value2) {
      int compare = Long.valueOf(value1).compareTo(value2);
      if (compare < 0) {
         return InequalVersionComparisonResult.BEFORE;               
      } else if (compare == 0) {
         return InequalVersionComparisonResult.EQUAL;
      }
      return InequalVersionComparisonResult.AFTER;
   }

   @Override
   public String toString() {
      return "viewId=" + viewId +
            ", nodeIndex=" + nodeIndex +
            '}';
   }
}
