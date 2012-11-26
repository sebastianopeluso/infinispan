package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface GMUVersionGenerator extends VersionGenerator {
   GMUEntryVersion mergeAndMax(Collection<? extends EntryVersion> entryVersions);

   GMUEntryVersion calculateCommitVersion(EntryVersion prepareVersion, Collection<Address> affectedOwners);

   GMUEntryVersion convertVersionToWrite(EntryVersion version);

   GMUEntryVersion calculateMaxVersionToRead(EntryVersion transactionVersion, Collection<Address> alreadyReadFrom);

   GMUEntryVersion calculateMinVersionToRead(EntryVersion transactionVersion, Collection<Address> alreadyReadFrom);

   GMUEntryVersion setNodeVersion(EntryVersion version, long value);

   ClusterSnapshot getClusterSnapshot(int viewId);

   Address getAddress();
}
