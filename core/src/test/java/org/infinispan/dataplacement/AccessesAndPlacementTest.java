package org.infinispan.dataplacement;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.topK.StreamLibContainer;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.0
 */
@Test(groups = "functional", testName = "dataplacement.AccessesAndPlacementTest")
public class AccessesAndPlacementTest {

   public void testNoMovement() {
      List<Address> members = createMembers(4);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.updateMembersList(members);
      Map<?, ?> newOwners = manager.calculateObjectsToMove();
      assert newOwners.isEmpty();
   }

   @SuppressWarnings("AssertWithSideEffects")
   public void testReturnValue() {
      List<Address> members = createMembers(2);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.updateMembersList(members);

      assert !manager.aggregateRequest(members.get(0), new ObjectRequest(null, null));
      assert manager.aggregateRequest(members.get(1), new ObjectRequest(null, null));
      assert !manager.aggregateRequest(members.get(0), new ObjectRequest(null, null));
      assert !manager.aggregateRequest(members.get(1), new ObjectRequest(null, null));
   }

   public void testObjectPlacement() {
      List<Address> members = createMembers(4);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.updateMembersList(members);

      Map<Object, Long> request = new HashMap<Object, Long>();

      TestKey key1 = new TestKey(1, members.get(0), members.get(1));
      TestKey key2 = new TestKey(2, members.get(2), members.get(3));

      request.put(key1, 1L);

      manager.aggregateRequest(members.get(2), new ObjectRequest(request, null));

      request = new HashMap<Object, Long>();
      request.put(key1, 1L);

      manager.aggregateRequest(members.get(3), new ObjectRequest(request, null));

      request = new HashMap<Object, Long>();
      request.put(key2, 1L);

      manager.aggregateRequest(members.get(0), new ObjectRequest(request, null));

      request = new HashMap<Object, Long>();
      request.put(key2, 1L);

      manager.aggregateRequest(members.get(1), new ObjectRequest(request, null));

      Map<Object, OwnersInfo> newOwners = manager.calculateObjectsToMove();

      assert newOwners.size() == 2;

      assertOwner(newOwners.get(key1), 2, 3);
      assertOwner(newOwners.get(key2), 0, 1);
   }

   public void testObjectPlacement2() {
      List<Address> members = createMembers(4);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.updateMembersList(members);

      Map<Object, Long> remote = new HashMap<Object, Long>();
      Map<Object, Long> local = new HashMap<Object, Long>();

      TestKey key1 = new TestKey(1, members.get(0), members.get(1));
      TestKey key2 = new TestKey(2, members.get(2), members.get(3));

      remote.put(key2, 1L);
      local.put(key1, 4L);

      manager.aggregateRequest(members.get(0), new ObjectRequest(remote, local));

      remote = new HashMap<Object, Long>();
      local = new HashMap<Object, Long>();

      remote.put(key2, 3L);
      local.put(key1, 1L);

      manager.aggregateRequest(members.get(1), new ObjectRequest(remote, local));

      remote = new HashMap<Object, Long>();
      local = new HashMap<Object, Long>();

      remote.put(key1, 2L);
      local.put(key2, 4L);

      manager.aggregateRequest(members.get(2), new ObjectRequest(remote, local));

      remote = new HashMap<Object, Long>();
      local = new HashMap<Object, Long>();

      remote.put(key1, 3L);
      local.put(key2, 2L);

      manager.aggregateRequest(members.get(3), new ObjectRequest(remote, null));

      Map<Object, OwnersInfo> newOwners = manager.calculateObjectsToMove();

      assert newOwners.size() == 2;

      assertOwner(newOwners.get(key1), 0, 3);
      assertOwner(newOwners.get(key2), 1, 2);
   }

   public void testObjectPlacement3() {
      List<Address> members = createMembers(4);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.updateMembersList(members);

      Map<Object, Long> request = new HashMap<Object, Long>();

      TestKey key = new TestKey(2, members.get(2), members.get(3));

      request.put(key, 2L);

      manager.aggregateRequest(members.get(0), new ObjectRequest(request, null));

      request = new HashMap<Object, Long>();

      request.put(key, 3L);

      manager.aggregateRequest(members.get(1), new ObjectRequest(request, null));

      request = new HashMap<Object, Long>();

      request.put(key, 5L);

      manager.aggregateRequest(members.get(2), new ObjectRequest(null, request));

      request = new HashMap<Object, Long>();

      request.put(key, 6L);

      manager.aggregateRequest(members.get(3), new ObjectRequest(null, request));

      Map<Object, OwnersInfo> newOwners = manager.calculateObjectsToMove();

      assert newOwners.isEmpty();
   }

   public void testRemoteAccesses() {
      List<Address> members = createMembers(4);
      RemoteAccessesManager manager = createRemoteAccessManager();
      StreamLibContainer container = StreamLibContainer.getInstance();
      container.setActive(true);
      container.setCapacity(5);
      container.resetAll();

      TestKey key1 = new TestKey(1, members.get(0), members.get(1));
      TestKey key2 = new TestKey(2, members.get(1), members.get(2));
      TestKey key3 = new TestKey(3, members.get(2), members.get(3));
      TestKey key4 = new TestKey(4, members.get(3), members.get(0));

      addKey(key1, false, 10, container);
      addKey(key2, true, 5, container);
      addKey(key3, true, 15, container);
      addKey(key4, false, 2, container);

      Map<Object, Long> remote = new HashMap<Object, Long>();
      Map<Object, Long> local = new HashMap<Object, Long>();

      local.put(key1, 10L);
      local.put(key4, 2L);

      assertAccesses(manager.getObjectRequestForAddress(members.get(0)), remote, local);

      remote.clear();
      local.clear();

      local.put(key1, 10L);
      remote.put(key2, 5L);

      assertAccesses(manager.getObjectRequestForAddress(members.get(1)), remote, local);

      remote.clear();
      local.clear();

      remote.put(key2, 5L);
      remote.put(key3, 15L);

      assertAccesses(manager.getObjectRequestForAddress(members.get(2)), remote, local);

      remote.clear();
      local.clear();

      local.put(key4, 2L);
      remote.put(key3, 15L);

      assertAccesses(manager.getObjectRequestForAddress(members.get(3)), remote, local);
   }

   private void assertAccesses(ObjectRequest request, Map<Object, Long> remote, Map<Object, Long> local) {
      Map<Object, Long> remoteAccesses = request.getRemoteAccesses();
      Map<Object, Long> localAccesses = request.getLocalAccesses();

      assert remoteAccesses.size() == remote.size();
      assert localAccesses.size() == local.size();

      for (Map.Entry<Object, Long> entry: remote.entrySet()) {
         long value1 = entry.getValue();
         long value2 = remoteAccesses.get(entry.getKey());

         assert value1 == value2;
      }

      for (Map.Entry<Object, Long> entry: local.entrySet()) {
         long value1 = entry.getValue();
         long value2 = localAccesses.get(entry.getKey());

         assert value1 == value2;
      }
   }

   private void addKey(Object key, boolean remote, int count, StreamLibContainer container) {
      for (int i = 0; i < count; ++i) {
         container.addGet(key, remote);
      }
   }

   private void assertOwner(OwnersInfo ownersInfo, Integer... newOwners) {
      assert ownersInfo != null;

      List<Integer> expectedOwners = Arrays.asList(newOwners);
      List<Integer> owners = ownersInfo.getNewOwnersIndexes();

      assert expectedOwners.size() == owners.size();
      assert expectedOwners.containsAll(owners);
   }

   private List<Address> createMembers(int size) {
      List<Address> members = new ArrayList<Address>(size);
      for (int i = 0; i < size; ++i) {
         members.add(new TestAddress(i));
      }
      return members;
   }

   private ObjectPlacementManager createObjectPlacementManager() {
      DefaultConsistentHash consistentHash = mock(DefaultConsistentHash.class);
      when(consistentHash.locate(isA(TestKey.class), anyInt())).thenAnswer(new Answer<List<Address>>() {
         @Override
         public List<Address> answer(InvocationOnMock invocationOnMock) throws Throwable {
            return new LinkedList<Address>(((TestKey) invocationOnMock.getArguments()[0]).getOwners());
         }
      });

      DistributionManager distributionManager = mock(DistributionManager.class);
      when(distributionManager.locate(isA(TestKey.class))).thenAnswer(new Answer<List<Address>>() {
         @Override
         public List<Address> answer(InvocationOnMock invocationOnMock) throws Throwable {
            return new LinkedList<Address>(((TestKey) invocationOnMock.getArguments()[0]).getOwners());
         }
      });
      when(distributionManager.getConsistentHash()).thenReturn(consistentHash);
      return new ObjectPlacementManager(distributionManager, new MurmurHash3(), 2);
   }

   private RemoteAccessesManager createRemoteAccessManager() {
      DefaultConsistentHash consistentHash = mock(DefaultConsistentHash.class);
      when(consistentHash.locate(isA(TestKey.class), anyInt())).thenAnswer(new Answer<List<Address>>() {
         @Override
         public List<Address> answer(InvocationOnMock invocationOnMock) throws Throwable {
            return new LinkedList<Address>(((TestKey) invocationOnMock.getArguments()[0]).getOwners());
         }
      });

      DistributionManager distributionManager = mock(DistributionManager.class);
      when(distributionManager.locate(isA(TestKey.class))).thenAnswer(new Answer<List<Address>>() {
         @Override
         public List<Address> answer(InvocationOnMock invocationOnMock) throws Throwable {
            return new LinkedList<Address>(((TestKey) invocationOnMock.getArguments()[0]).getOwners());
         }
      });
      when(distributionManager.getConsistentHash()).thenReturn(consistentHash);
      return new RemoteAccessesManager(distributionManager);
   }

   private class TestKey {

      private final Collection<Address> owners;
      private final int id;

      private TestKey(int id, Address... owners) {
         this.id = id;
         this.owners = Arrays.asList(owners);
      }

      public Collection<Address> getOwners() {
         return owners;
      }

      public int getId() {
         return id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TestKey testKey = (TestKey) o;

         return id == testKey.id;

      }

      @Override
      public int hashCode() {
         return id;
      }

      @Override
      public String toString() {
         return "TestKey{" +
               "id=" + id +
               '}';
      }
   }

}
