/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.tx.dld;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.concurrent.ResponseFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class ControlledRpcManager implements RpcManager {

   private static final Log log = LogFactory.getLog(ControlledRpcManager.class);

   private final ReclosableLatch replicationLatch = new ReclosableLatch(true);
   private final ReclosableLatch blockingLatch = new ReclosableLatch(true);
   private volatile Set<Class> blockBeforeFilter = Collections.emptySet();
   private volatile Set<Class> blockAfterFilter = Collections.emptySet();

   private volatile Set<Class> failBeforeFilter = Collections.emptySet();
   private volatile Set<Class> failAfterFilter = Collections.emptySet();

   protected RpcManager realOne;

   public ControlledRpcManager(RpcManager realOne) {
      this.realOne = realOne;
   }

   public void failBeforeFor(Class... filter) {
      this.failBeforeFilter = new HashSet<Class>(Arrays.asList(filter));
   }

   public void failAfterFor(Class... filter) {
      this.failAfterFilter = new HashSet<Class>(Arrays.asList(filter));
   }

   public void stopFailing() {
      this.failBeforeFilter = Collections.emptySet();
      this.failAfterFilter = Collections.emptySet();
   }

   public void blockBefore(Class... filter) {
      this.blockBeforeFilter = new HashSet<Class>(Arrays.asList(filter));
      replicationLatch.close();
      blockingLatch.close();
   }

   public void blockAfter(Class... filter) {
      this.blockAfterFilter = new HashSet<Class>(Arrays.asList(filter));
      replicationLatch.close();
      blockingLatch.close();
   }

   public void stopBlocking() {
      blockBeforeFilter = Collections.emptySet();
      blockAfterFilter = Collections.emptySet();
      replicationLatch.open();
   }

   public void waitForCommandToBlock() throws InterruptedException {
      blockingLatch.await();
   }

   protected void waitBefore(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockBeforeFilter);
   }

   protected void waitAfter(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockAfterFilter);
   }

   protected void waitForReplicationLatch(ReplicableCommand rpcCommand, Set<Class> filter) {
      Class cmdClass = getActualClass(rpcCommand);
      if (!filter.contains(cmdClass)) {
         return;
      }

      try {
         log.debugf("Replication trigger called, waiting for latch to open.");
         blockingLatch.open();
         replicationLatch.await();
         log.trace("Replication latch opened, continuing.");
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception!", e);
      }
   }

   private Class getActualClass(ReplicableCommand rpcCommand) {
      Class cmdClass = rpcCommand.getClass();
      if (cmdClass.equals(SingleRpcCommand.class)) {
         cmdClass = ((SingleRpcCommand) rpcCommand).getCommand().getClass();
      }
      return cmdClass;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder) {
      log.trace("invokeRemotely1");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, totalOrder);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
      return responseMap;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, boolean totalOrder) {
      log.trace("invokeRemotely2");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, totalOrder);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
      return responseMap;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean totalOrder) {
      log.trace("invokeRemotely3");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, totalOrder);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
      return responseMap;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean sync, boolean totalOrder) throws RpcException {
      log.trace("invokeRemotely4");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responseMap = realOne.invokeRemotely(recipients, rpcCommand, sync, totalOrder);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
      return responseMap;
   }

   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean sync, boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      log.trace("invokeRemotely5");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      Map<Address, Response> responses = realOne.invokeRemotely(recipients, rpcCommand, sync, usePriorityQueue, totalOrder);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
      return responses;
   }

   @Override
   public ResponseFuture invokeRemotelyWithFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, boolean totalOrder) {
      log.trace("invokeRemotely6");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      ResponseFuture responses = realOne.invokeRemotelyWithFuture(recipients, rpcCommand, usePriorityQueue, totalOrder);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
      return responses;
   }

   public void broadcastRpcCommand(ReplicableCommand rpcCommand, boolean sync, boolean totalOrder) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand1");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.broadcastRpcCommand(rpcCommand, sync, totalOrder);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
   }

   public void broadcastRpcCommand(ReplicableCommand rpcCommand, boolean sync, boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      log.trace("ControlledRpcManager.broadcastRpcCommand2");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.broadcastRpcCommand(rpcCommand, sync, usePriorityQueue, totalOrder);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
   }

   public void broadcastRpcCommandInFuture(ReplicableCommand rpcCommand, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture1");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.broadcastRpcCommandInFuture(rpcCommand, future);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
   }

   public void broadcastRpcCommandInFuture(ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.broadcastRpcCommandInFuture2");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.broadcastRpcCommandInFuture(rpcCommand, usePriorityQueue, future);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture1");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, future);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture2");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture3");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future, timeout);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      log.trace("ControlledRpcManager.invokeRemotelyInFuture4");
      failBeforeIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      realOne.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future, timeout, ignoreLeavers);
      failAfterIfNeeded(rpcCommand);
      waitAfter(rpcCommand);
   }

   public Transport getTransport() {
      return realOne.getTransport();
   }

   public Address getAddress() {
      return realOne.getAddress();
   }

   public void failBeforeIfNeeded(ReplicableCommand rpcCommand) {
      if (failBeforeFilter.contains(getActualClass(rpcCommand))) {
         throw new IllegalStateException("Induced failure!");
      }
   }

   public void failAfterIfNeeded(ReplicableCommand rpcCommand) {
      if (failAfterFilter.contains(getActualClass(rpcCommand))) {
         throw new IllegalStateException("Induced failure!");
      }
   }

   @Override
   public int getTopologyId() {
      return realOne.getTopologyId();
   }

   @Override
   public List<Address> getMembers() {
      return realOne.getMembers();
   }
}
