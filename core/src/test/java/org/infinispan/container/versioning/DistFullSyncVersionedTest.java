package org.infinispan.container.versioning;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(testName = "container.versioning.DistFullSyncVersionedTest", groups = "functional")
public class DistFullSyncVersionedTest extends ReplPrepareSyncVersionedTest {

   public DistFullSyncVersionedTest() {
      super(3, CacheMode.DIST_SYNC, true);
   }
}
