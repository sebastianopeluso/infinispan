/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
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
package org.infinispan.interceptors.gmu;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheValue;
import org.infinispan.container.gmu.L1GMUContainer;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.VersionNotAvailableException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.transaction.gmu.GMUHelper.*;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @author Hugo Pimentel
 * @since 5.2
 */
public class GMUDistributionInterceptor extends TxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(GMUDistributionInterceptor.class);
   protected GMUVersionGenerator versionGenerator;
   private L1GMUContainer l1GMUContainer;
   private CommitLog commitLog;

   @Inject
   public void setVersionGenerator(VersionGenerator versionGenerator, L1GMUContainer l1GMUContainer, CommitLog commitLog) {
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      this.l1GMUContainer = l1GMUContainer;
      this.commitLog = commitLog;
   }

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      Map<Address, Response> responses = rpcManager.invokeRemotely(recipients, command, true, true, false);
      log.debugf("prepare command for transaction %s is sent. responses are: %s",
                 command.getGlobalTransaction().globalId(), responses.toString());

      joinAndSetTransactionVersion(responses.values(), ctx, versionGenerator);
   }

   @Override
   protected InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command)
         throws Exception {
      if (isL1CacheEnabled && ctx instanceof TxInvocationContext) {
         if (log.isTraceEnabled()) {
            log.tracef("Trying to retrieve a the key %s from L1 GMU Data Container", key);
         }
         TxInvocationContext txInvocationContext = (TxInvocationContext) ctx;
         InternalGMUCacheEntry gmuCacheEntry = l1GMUContainer.getValidVersion(key,
                                                                              txInvocationContext.getTransactionVersion(),
                                                                              txInvocationContext.getAlreadyReadFrom());
         if (gmuCacheEntry != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Retrieve a L1 entry for key %s: %s", key, gmuCacheEntry);
            }
            txInvocationContext.addKeyReadInCommand(key, gmuCacheEntry);
            txInvocationContext.addReadFrom(dm.getPrimaryLocation(key));
            return gmuCacheEntry.getInternalCacheEntry();
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Failed to retrieve  a L1 entry for key %s", key);
      }
      return performRemoteGet(key, ctx, acquireRemoteLock, command);
   }

   @Override
   protected void storeInL1(Object key, InternalCacheEntry ice, InvocationContext ctx, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Doing a put in L1 into the L1 GMU Data Container");
      }
      InternalGMUCacheEntry gmuCacheEntry = ctx.getKeysReadInCommand().get(key);
      if (gmuCacheEntry == null) {
         throw new NullPointerException("GMU cache entry cannot be null");
      }
      l1GMUContainer.insertOrUpdate(key, gmuCacheEntry);
      CacheEntry ce = ctx.lookupEntry(key);
      if (ce == null || ce.isNull() || ce.isLockPlaceholder() || ce.getValue() == null) {
         if (ce != null && ce.isChanged()) {
            ce.setValue(ice.getValue());
         } else {
            if (isWrite)
               entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
            else
               ctx.putLookedUpEntry(key, ice);
         }
      }
   }

   private InternalCacheEntry performRemoteGet(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command) throws Exception {
      if (ctx instanceof SingleKeyNonTxInvocationContext) {
         return retrieveSingleKeyFromRemoteSource(key, (SingleKeyNonTxInvocationContext) ctx, command);
      } else if (ctx instanceof TxInvocationContext) {
         return retrieveTransactionalGetFromRemoteSource(key, (TxInvocationContext) ctx, acquireRemoteLock, command);
      }
      throw new IllegalStateException("Only handles transaction context or single key gets");
   }

   private InternalCacheEntry retrieveTransactionalGetFromRemoteSource(Object key, TxInvocationContext txInvocationContext,
                                                                       boolean acquireRemoteLock, FlagAffectedCommand command) {
      GlobalTransaction gtx = acquireRemoteLock ? txInvocationContext.getGlobalTransaction() : null;

      List<Address> targets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key));      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      targets.retainAll(rpcManager.getTransport().getMembers());

      Collection<Address> alreadyReadFrom = txInvocationContext.getAlreadyReadFrom();
      GMUVersion transactionVersion = toGMUVersion(txInvocationContext.getTransactionVersion());

      BitSet alreadyReadFromMask;

      if (alreadyReadFrom == null) {
         alreadyReadFromMask = null;
      } else {
         int txViewId = transactionVersion.getViewId();
         ClusterSnapshot clusterSnapshot = versionGenerator.getClusterSnapshot(txViewId);
         alreadyReadFromMask = new BitSet(clusterSnapshot.size());

         for (Address address : alreadyReadFrom) {
            int idx = clusterSnapshot.indexOf(address);
            if (idx != -1) {
               alreadyReadFromMask.set(idx);
            }
         }
      }

      ClusteredGetCommand get = cf.buildGMUClusteredGetCommand(key, command.getFlags(), acquireRemoteLock,
                                                               gtx, transactionVersion, alreadyReadFromMask);

      ClusterGetResponse response = doRemote(targets, get);
      InternalGMUCacheValue gmuCacheValue = convert(response.value, InternalGMUCacheValue.class);

      InternalGMUCacheEntry gmuCacheEntry = (InternalGMUCacheEntry) gmuCacheValue.toInternalCacheEntry(key);
      txInvocationContext.addKeyReadInCommand(key, gmuCacheEntry);
      txInvocationContext.addReadFrom(response.sender);

      if (log.isDebugEnabled()) {
         log.debugf("Remote Get successful for transaction %s and key %s. Return value is %s",
                    txInvocationContext.getGlobalTransaction().globalId(), key, gmuCacheValue);
      }
      return gmuCacheEntry;
   }

   private InternalCacheEntry retrieveSingleKeyFromRemoteSource(Object key, SingleKeyNonTxInvocationContext ctx, FlagAffectedCommand command) {
      List<Address> targets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key));      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      targets.retainAll(rpcManager.getTransport().getMembers());

      ClusteredGetCommand get = cf.buildGMUClusteredGetCommand(key, command.getFlags(), false, null,
                                                               toGMUVersion(commitLog.getCurrentVersion()), null);

      ClusterGetResponse response = doRemote(targets, get);
      InternalGMUCacheValue gmuCacheValue = convert(response.value, InternalGMUCacheValue.class);
      InternalGMUCacheEntry gmuCacheEntry = (InternalGMUCacheEntry) gmuCacheValue.toInternalCacheEntry(key);
      ctx.addKeyReadInCommand(key, gmuCacheEntry);

      if (log.isDebugEnabled()) {
         log.debugf("Remote Get successful for single key %s. Return value is %s", key, gmuCacheValue);
      }
      return gmuCacheEntry;
   }

   private ClusterGetResponse doRemote(List<Address> targets, ClusteredGetCommand command) {
      final Address localAddress = rpcManager.getAddress();
      final long spinTimeout = 1000;
      final Set<Address> alreadyTargeted = new HashSet<Address>();
      final Object key = command.getKey();
      while (!targets.isEmpty()) {
         Address target = targets.remove(0);
         alreadyTargeted.add(target);
         try {
            Object result = null;
            boolean local = target.equals(localAddress);
            if (log.isDebugEnabled()) {
               log.debugf("Perform remote get for key [%s]. Command=%s, target=%s, local?=%s",
                          key, command, target, local);
            }
            if (local) {
               cf.initializeReplicableCommand(command, true);
               result = command.perform(null);
            } else {
               Collection<Address> reallyTargets = Collections.singleton(target);
               ResponseFilter filter = new ClusteredGetResponseValidityFilter(reallyTargets, localAddress);
               Map<Address, Response> responses = rpcManager.invokeRemotely(reallyTargets, command, ResponseMode.WAIT_FOR_VALID_RESPONSE,
                                                                            spinTimeout, true, filter, false);
               Response r = responses.get(target);
               if (r == null) {
                  continue;
               }
               if (r instanceof SuccessfulResponse) {
                  result = ((SuccessfulResponse) r).getResponseValue();
               }
            }
            if (log.isDebugEnabled()) {
               log.debugf("Remote get done for key [%s]. Target=%s, local?=%s, response=%s",
                          key, target, local, result);
            }
            if (result instanceof InternalCacheValue) {
               return new ClusterGetResponse(target, (InternalCacheValue) result);
            } else if (result instanceof Collection) {
               Collection<Address> oldOwners = (Collection<Address>) result;
               for (Address oldOwner : oldOwners) {
                  if (!rpcManager.getMembers().contains(oldOwner)) {
                     alreadyTargeted.add(oldOwner); //owner already left the cluster
                  } else if (!alreadyTargeted.contains(oldOwner)) {
                     targets.add(oldOwner);
                  }
               }
            }
            if (log.isDebugEnabled()) {
               log.debugf("Value not found for key [%s]. Owners remaining: %s", key, targets);
            }
         } catch (Throwable throwable) {
            //no-op
         }
      }
      throw new VersionNotAvailableException(command.getKey());
   }

   private class ClusterGetResponse {
      private final Address sender;
      private final InternalCacheValue value;

      private ClusterGetResponse(Address sender, InternalCacheValue value) {
         this.sender = sender;
         this.value = value;
      }
   }
}
