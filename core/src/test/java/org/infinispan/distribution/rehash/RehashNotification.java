package org.infinispan.distribution.rehash;

import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Data Rehash Event: checks the information in the event
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "distribution.rehash.RehashNotification")
public class RehashNotification extends BaseDistFunctionalTest {

   private final DataRehashListener dataRehashListener = new DataRehashListener();

   public RehashNotification() {
      INIT_CLUSTER_SIZE = 1;
      performRehashing = true;
   }

   public void testNotification() throws InterruptedException {
      LinkedList<Address> oldCacheSet = new LinkedList<Address>();
      LinkedList<Address> newCacheSet = new LinkedList<Address>();

      cache(0, cacheName).getAdvancedCache().addListener(dataRehashListener);
      cache(0, cacheName).put("k1", "v1");
      cache(0, cacheName).put("k2", "v2");
      cache(0, cacheName).put("k3", "v3");

      long expectedViewId = 2;

      addClusterEnabledCacheManager(buildConfiguration());
      waitForJoinTasksToComplete(60000, cache(1, cacheName));

      oldCacheSet.addLast(cache(0, cacheName).getAdvancedCache().getRpcManager().getAddress());

      newCacheSet.addLast(cache(0, cacheName).getAdvancedCache().getRpcManager().getAddress());
      newCacheSet.addLast(cache(1, cacheName).getAdvancedCache().getRpcManager().getAddress());

      doAssertions(oldCacheSet, newCacheSet, expectedViewId++);

      addClusterEnabledCacheManager(buildConfiguration());
      waitForJoinTasksToComplete(60000, cache(2, cacheName));

      oldCacheSet.addLast(cache(1, cacheName).getAdvancedCache().getRpcManager().getAddress());
      newCacheSet.addLast(cache(2, cacheName).getAdvancedCache().getRpcManager().getAddress());

      doAssertions(oldCacheSet, newCacheSet, expectedViewId);
   }

   private void doAssertions(Collection oldCacheSetExpected, Collection newCacheSetExpected, long expectedViewId)
         throws InterruptedException {
      dataRehashListener.awaitEvent(60000);

      assert dataRehashListener.oldCacheSet.size() == oldCacheSetExpected.size() : "Old cache set size mismatch." +
            "In event size: " + dataRehashListener.oldCacheSet.size() + " and expected size: " + oldCacheSetExpected.size();
      assert dataRehashListener.newCacheSet.size() == newCacheSetExpected.size() : "New cache set size mismatch." +
            "In event size: " + dataRehashListener.newCacheSet.size() + " and expected size: " + newCacheSetExpected.size();

      assert dataRehashListener.oldCacheSet.containsAll(oldCacheSetExpected) : "Old cache set in Event does not " +
            "contains all the old cache set members";
      assert !dataRehashListener.oldCacheSet.containsAll(newCacheSetExpected) : "Old cache set in Event contains some " +
            "new cache set members";

      assert dataRehashListener.newCacheSet.containsAll(oldCacheSetExpected) : "New cache set in Event does not " +
            "contains all the old cache set members";
      assert dataRehashListener.newCacheSet.containsAll(newCacheSetExpected) : "New cache set in Event does not " +
            "contains all the new cache set members";

      assert dataRehashListener.viewId == expectedViewId : "View Id mismatch";
   }

   @Listener
   public static class DataRehashListener {

      private Collection oldCacheSet;
      private Collection newCacheSet;
      private long viewId;
      private boolean eventReceived = false;

      @DataRehashed
      public synchronized void handle(DataRehashedEvent event) {
         if (!event.isPre()) {
            oldCacheSet = event.getMembersAtStart();
            newCacheSet = event.getMembersAtEnd();
            viewId = event.getNewViewId();
            eventReceived = true;
         }
      }

      public synchronized void awaitEvent(long timeout) throws InterruptedException {
         long giveUp = System.currentTimeMillis() + timeout;

         while (!eventReceived && giveUp > System.currentTimeMillis()) {
            wait(100);
         }
         assert eventReceived : "Event expected!";
         eventReceived = false;
      }
   }
}
