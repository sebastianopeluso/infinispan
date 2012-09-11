package org.infinispan.container.versioning;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(testName = "container.versioning.ReplFullSyncVersionedTest", groups = "functional")
public class ReplFullSyncVersionedTest extends ReplPrepareSyncVersionedTest {

   public ReplFullSyncVersionedTest() {
      super(3, CacheMode.REPL_SYNC, true);
   }
}
