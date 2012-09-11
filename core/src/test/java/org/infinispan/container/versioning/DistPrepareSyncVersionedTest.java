package org.infinispan.container.versioning;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(testName = "container.versioning.DistPrepareSyncVersionedTest", groups = "functional")
public class DistPrepareSyncVersionedTest extends ReplPrepareSyncVersionedTest {

   public DistPrepareSyncVersionedTest() {
      super(3, CacheMode.DIST_SYNC, false);
   }

   @Override
   protected void assertCacheValue(int originatorIndex, Object key, Object value) {
      for (int index = 0; index < caches().size(); ++index) {
         //we should use eventually because of the commit phase is asynchronous and the key can be remote. if not used
         //can originate assertion errors
         assertEventuallyEquals(index, key, value);
      }
   }
}
