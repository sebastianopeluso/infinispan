package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public interface GMUVersionGenerator extends VersionGenerator {
   IncrementableEntryVersion mergeAndMax(Collection<? extends EntryVersion> entryVersions);

   IncrementableEntryVersion calculateCommitVersion(EntryVersion prepareVersion,
                                       Collection<Address> affectedOwners);

   IncrementableEntryVersion convertVersionToWrite(EntryVersion version);

   IncrementableEntryVersion calculateMaxVersionToRead(EntryVersion transactionVersion,
                                                       Collection<Address> alreadyReadFrom);

   IncrementableEntryVersion calculateMinVersionToRead(EntryVersion transactionVersion,
                                                       Collection<Address> alreadyReadFrom);

   ClusterSnapshot getClusterSnapshot(long viewId);

   Address getAddress();
}
