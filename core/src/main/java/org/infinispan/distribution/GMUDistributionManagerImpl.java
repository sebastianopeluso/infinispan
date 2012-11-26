/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.infinispan.transaction.gmu.GMUHelper.convert;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * The distribution manager implementation for the GMU protocol
 *
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
@MBean(objectName = "GMUDistributionManager", description = "Component that handles distribution of content across a cluster")
public class GMUDistributionManagerImpl extends DistributionManagerImpl {

   private static final Log log = LogFactory.getLog(GMUDistributionManagerImpl.class);

   private GMUVersionGenerator versionGenerator;

   /**
    * Default constructor
    */
   public GMUDistributionManagerImpl() {}

   @Inject
   public void setVersionGenerator(VersionGenerator versionGenerator) {
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   @Override
   public InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock) throws Exception {
      if (!ctx.isInTxScope()) {
         throw new IllegalStateException("Only handles transaction context");
      }

      TxInvocationContext txInvocationContext = convert(ctx, TxInvocationContext.class);
      GlobalTransaction gtx = acquireRemoteLock ? ((TxInvocationContext)ctx).getGlobalTransaction() : null;

      List<Address> targets = new ArrayList<Address>(locate(key));
      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      targets.retainAll(rpcManager.getTransport().getMembers());

      Collection<Address> alreadyReadFrom = txInvocationContext.getAlreadyReadFrom();
      EntryVersion transactionVersion = txInvocationContext.getTransactionVersion();

      EntryVersion maxVersionToRead = versionGenerator.calculateMaxVersionToRead(transactionVersion, alreadyReadFrom);
      EntryVersion minVersionToRead = versionGenerator.calculateMinVersionToRead(transactionVersion, alreadyReadFrom);

      ClusteredGetCommand get = cf.buildGMUClusteredGetCommand(key, ctx.getFlags(), acquireRemoteLock, gtx, minVersionToRead, maxVersionToRead, null);

      if(log.isDebugEnabled()) {
         log.debugf("Perform a remote get for transaction %s. Key: %s, minVersion: %s, maxVersion: %s",
                    txInvocationContext.getGlobalTransaction().prettyPrint(), key, minVersionToRead, maxVersionToRead);
      }

      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets, getAddress());
      Map<Address, Response> responses = rpcManager.invokeRemotely(targets, get, ResponseMode.WAIT_FOR_VALID_RESPONSE,
                                                                   configuration.getSyncReplTimeout(), true, filter, false);

      if(ctx.isInTxScope()) {
         if(log.isDebugEnabled()) {
            log.debugf("Remote get done for transaction %s [key:%s]. response are: %s",
                       ((LocalTxInvocationContext) ctx).getGlobalTransaction().prettyPrint(),
                       key, responses);
         }
      } else {
         if(log.isDebugEnabled()) {
            log.debugf("Remote get done for non-transaction context [key:%s]. response are: %s", key, responses);
         }
      }

      if (!responses.isEmpty()) {
         for (Map.Entry<Address,Response> entry : responses.entrySet()) {
            Response r = entry.getValue();
            if (r == null) {
               continue;
            }
            if (r instanceof SuccessfulResponse) {
               InternalGMUCacheValue gmuCacheValue = convert(((SuccessfulResponse) r).getResponseValue(),
                                                             InternalGMUCacheValue.class);

               InternalGMUCacheEntry gmuCacheEntry = (InternalGMUCacheEntry) gmuCacheValue.toInternalCacheEntry(key);
               txInvocationContext.addKeyReadInCommand(key, gmuCacheEntry);
               txInvocationContext.addReadFrom(entry.getKey());

               if(log.isDebugEnabled()) {
                  log.debugf("Remote Get successful for transaction %s and key %s. Return value is %s",
                             ((LocalTxInvocationContext) ctx).getGlobalTransaction().prettyPrint(), key, gmuCacheValue);
               }
               return gmuCacheEntry;
            }
         }
      }

      // TODO If everyone returned null, and the read CH has changed, retry the remote get.
      // Otherwise our get command might be processed by the old owners after they have invalidated their data
      // and we'd return a null even though the key exists on
      return null;
   }
}
