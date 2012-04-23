package org.infinispan.tx.totalorder;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.DistSimpleTotalOrder1Pc2Test")
public class DistSimpleTotalOrder1Pc2Test extends DistSimpleTotalOrder1PcTest {

   public DistSimpleTotalOrder1Pc2Test() {
      this.clusterSize = 3;
   }
}
