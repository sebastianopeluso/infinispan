package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.reconfigurableprotocol.protocol.TotalOrderCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TwoPhaseCommitProtocol;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.sleepThread;
import static org.testng.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.LocalModeSelfTunningTest")
public class LocalModeSelfTunningTest extends SingleCacheManagerTest {

   private static final String KEY_1 = "KEY_1";
   private static final String KEY_2 = "KEY_2";
   private static final String KEY_3 = "KEY_3";
   private static final String VALUE = "VALUE";
   protected final ConfigurationBuilder builder;

   public LocalModeSelfTunningTest() {
      builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testSwitch() throws Exception {
      populate();
      ReconfigurableReplicationManager manager = extractComponent(cache, ReconfigurableReplicationManager.class);

      assertProtocol(manager, TwoPhaseCommitProtocol.UID);
      assertEpoch(manager, 0);

      triggerTunningProtocol(manager, TotalOrderCommitProtocol.UID);

      assertProtocol(manager, TotalOrderCommitProtocol.UID);
      assertEpoch(manager, 1);

      assertKeysValue();
   }

   protected void populate() {
      cache.put(KEY_1, VALUE);
      cache.put(KEY_2, VALUE);
      cache.put(KEY_3, VALUE);
   }

   protected void assertKeysValue() {
      assertEquals(VALUE, cache.get(KEY_1));
      assertEquals(VALUE, cache.get(KEY_2));
      assertEquals(VALUE, cache.get(KEY_3));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   private void assertProtocol(final ReconfigurableReplicationManager manager, final String protocolId) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return manager.getCurrentProtocolId().equals(protocolId);
         }
      });
   }

   private void assertEpoch(final ReconfigurableReplicationManager manager, final long epoch) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return epoch == manager.getCurrentEpoch();
         }
      });
   }

   private void triggerTunningProtocol(ReconfigurableReplicationManager manager, String protocolId) throws Exception {
      sleepThread(manager.getSwitchCoolDownTime() * 1000);
      manager.switchTo(protocolId, false, false);
   }
}
