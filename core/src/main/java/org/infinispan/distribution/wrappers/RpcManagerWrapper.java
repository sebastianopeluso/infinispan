package org.infinispan.distribution.wrappers;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
public class RpcManagerWrapper implements RpcManager {
   private final RpcManager actual;

   public RpcManagerWrapper(RpcManager actual) {
      this.actual = actual;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder) {
      System.out.println("RpcManagerWrapper.invokeRemotely");
      Map<Address,Response> ret;
      long currentTime = System.nanoTime();

      try{
         ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, totalOrder);
         TransactionsStatisticsRegistry.addValue(IspnStats.RTT,System.nanoTime() - currentTime);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_SUCCESSFUL_RTTS);
         TransactionsStatisticsRegistry.addValue(IspnStats.NUM_NODES_IN_PREPARE,recipients.size());
      }
      catch(CacheException e){
         throw e;
      }
      catch(IllegalStateException i){
         throw i;
      }
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, boolean totalOrder) {
      System.out.println("RpcManagerWrapper.invokeRemotely _ 1");
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, totalOrder);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean totalOrder) {
      System.out.println("RpcManagerWrapper.invokeRemotely _ 2");
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, totalOrder);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      System.out.println("RpcManagerWrapper.broadcastRpcCommand");
      actual.broadcastRpcCommand(rpc, sync, totalOrder);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      System.out.println("DIE : broadcastRpcCommand");
      actual.broadcastRpcCommand(rpc, sync, usePriorityQueue, totalOrder);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      System.out.println("RpcManagerWrapper.broadcastRpcCommandInFuture");
      actual.broadcastRpcCommandInFuture(rpc, future);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      System.out.println("RpcManagerWrapper.broadcastRpcCommandInFuture");
      actual.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      System.out.println("RpcManagerWrapper.invokeRemotely _3");
      return actual.invokeRemotely(recipients, rpc, sync, totalOrder);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      System.out.println("RpcManagerWrapper.invokeRemotely _ 4 + Command "+rpc);
      return actual.invokeRemotely(recipients, rpc, sync, usePriorityQueue, totalOrder);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      System.out.println("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      System.out.println("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
      System.out.println("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      System.out.println("RpcManagerWrapper.invokeRemotelyInFuture");
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout, ignoreLeavers);
   }

   @Override
   public Transport getTransport() {
      System.out.println("RpcManagerWrapper.getTransport");
      return actual.getTransport();
   }

   @Override
   public List<Address> getMembers() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Address getAddress() {
      System.out.println("RpcManagerWrapper.getAddress");
      return actual.getAddress();
   }

   @Override
   public int getTopologyId() {
      return 0;  // TODO: Customise this generated block
   }
}
