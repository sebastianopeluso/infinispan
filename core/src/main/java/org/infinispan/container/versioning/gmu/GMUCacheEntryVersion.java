/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUCacheEntryVersion extends GMUVersion {

   private transient CommitLog.VersionEntry versionEntry;
   private transient long versionNumber;
   private final int subVersion;

   public GMUCacheEntryVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator, CommitLog.VersionEntry versionEntry, int subVersion) {
      super(cacheName, viewId, versionGenerator);
      this.versionEntry = versionEntry;
      this.subVersion = subVersion;
      this.versionNumber = versionEntry.getVersion().getThisNodeVersionValue();
   }

   private GMUCacheEntryVersion(String cacheName, int viewId, ClusterSnapshot clusterSnapshot,
                                CommitLog.VersionEntry versionEntry, int subVersion) {
      super(cacheName, viewId, clusterSnapshot);
      this.subVersion = subVersion;
   }

   @Override
   public final long getVersionValue(Address address) {
      return getThisNodeVersionValue();
   }

   @Override
   public final long getVersionValue(int addressIndex) {
      return getThisNodeVersionValue();
   }

   @Override
   public final long getThisNodeVersionValue() {
      return versionEntry.getVersion().getThisNodeVersionValue();
   }

   public final int getSubVersion() {
      return subVersion;
   }

   public CommitLog.VersionEntry getVersionEntryInCommitLog(){
      return versionEntry;
   }

   public void detachVersionEntry(){
      versionEntry = null;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      //this particular version can only be compared with this type of GMU version or with GMUReadVersion
      if (other == null) {
         return BEFORE;
      } else if (other instanceof GMUCacheEntryVersion) {
         GMUCacheEntryVersion cacheEntryVersion = (GMUCacheEntryVersion) other;
         if(versionEntry == null || cacheEntryVersion.versionEntry == null) return BEFORE;

         //InequalVersionComparisonResult result = versionEntry.getVersion().compareToWithCheckUnsafeBeforeOrEqual(cacheEntryVersion.versionEntry.getVersion());
          InequalVersionComparisonResult result = compare(versionNumber, cacheEntryVersion.versionNumber);
          if (result == EQUAL) {
            return compare(subVersion, cacheEntryVersion.subVersion);
         }
         return result;
      } else if (other instanceof GMUReadVersion) {
         GMUReadVersion readVersion = (GMUReadVersion) other;
         if(versionEntry == null) return BEFORE;
         if (readVersion.contains(versionEntry.getVersion().getThisNodeVersionValue(), subVersion)) {
            //this is an invalid version. set it higher
            return AFTER;
         }
         return compare(versionEntry.getVersion().getThisNodeVersionValue(), readVersion.getThisNodeVersionValue());
      } else if (other instanceof GMUReplicatedVersion) {
         GMUReplicatedVersion replicatedVersion = (GMUReplicatedVersion) other;
         if(versionEntry == null) return BEFORE;
         InequalVersionComparisonResult result = versionEntry.getVersion().compareToWithCheckUnsafeBeforeOrEqual(replicatedVersion);
         if (result == InequalVersionComparisonResult.UNSAFE_BEFORE_OR_EQUAL) {
            throw new IllegalArgumentException("GMU entry version cannot compare BeforeOrEqual" + versionEntry.getVersion() + " " + replicatedVersion);
         }
         if (result == EQUAL) {
            return compare(viewId, replicatedVersion.getViewId());
         }
         return result;
      } else if (other instanceof GMUDistributedVersion) {
         GMUDistributedVersion distributedVersion = (GMUDistributedVersion) other;
         if(versionEntry == null) return BEFORE;
         InequalVersionComparisonResult result = versionEntry.getVersion().compareToWithCheckUnsafeBeforeOrEqual(distributedVersion);

         if (result == InequalVersionComparisonResult.UNSAFE_BEFORE_OR_EQUAL) {
            throw new IllegalArgumentException("GMU entry version cannot compare BeforeOrEqual" + versionEntry.getVersion() + " " + distributedVersion);
         }
         if (result == EQUAL) {
            return compare(viewId, distributedVersion.getViewId());
         }
         return result;
      }
      throw new IllegalArgumentException("Cannot compare " + getClass() + " with " + other.getClass());
   }

   @Override
   public InequalVersionComparisonResult compareToWithCheckUnsafeBeforeOrEqual(EntryVersion other) {
      return this.compareTo(other);
   }

   @Override
   public String toString() {
      return "GMUCacheEntryVersion{" +
            "version=" + versionEntry +
            ", subVersion=" + subVersion +
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
         output.writeInt(object.subVersion);
         if(object.versionEntry != null){
            output.writeBoolean(true);

            output.writeObject(object.versionEntry.getVersion());
         }
         else{
            output.writeBoolean(false);
         }

      }

      @Override
      public GMUCacheEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = input.readUTF();
         int viewId = input.readInt();
         int subVersion = input.readInt();
         boolean attachVersionEntry = input.readBoolean();
         CommitLog.VersionEntry v = null;
         if(attachVersionEntry){
            GMUVersion version = (GMUVersion) input.readObject();
            v = CommitLog.generateVersionEntry(version, null, 0 , 0);
         }

         ClusterSnapshot clusterSnapshot = getClusterSnapshot(globalComponentRegistry, cacheName, viewId);
         return new GMUCacheEntryVersion(cacheName, viewId, clusterSnapshot, v, subVersion);
      }

      @Override
      public Integer getId() {
         return Ids.GMU_CACHE_VERSION;
      }
   }
}
