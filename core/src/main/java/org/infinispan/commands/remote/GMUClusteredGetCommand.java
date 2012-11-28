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
package org.infinispan.commands.remote;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUEntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUEntryVersion;

/**
 * Issues a remote get call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up the
 * {@link org.infinispan.interceptors.base.CommandInterceptor} chain.
 * <p/>
 *
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUClusteredGetCommand extends ClusteredGetCommand {

   public static final byte COMMAND_ID = 32;
   private static final Log log = LogFactory.getLog(GMUClusteredGetCommand.class);

   //with null, it does not waits for anything and return a value compatible with maxVC
   private EntryVersion minVersion = null;
   //read the most recent version (in tx context, this is not null)
   private EntryVersion maxVersion;
   private CommitLog commitLog;
   private Configuration configuration;

   private GMUClusteredGetCommand() {
      super(null); // For command id uniqueness test
   }

   public GMUClusteredGetCommand(String cacheName) {
      super(cacheName);
   }

   public GMUClusteredGetCommand(Object key, String cacheName, Set<Flag> flags, boolean acquireRemoteLock, GlobalTransaction gtx) {
      super(key, cacheName, flags, acquireRemoteLock, gtx);
   }

   public GMUClusteredGetCommand(Object key, String cacheName, Set<Flag> flags, boolean acquireRemoteLock,
                                 GlobalTransaction globalTransaction, EntryVersion minVersion, EntryVersion maxVersion) {
      super(key,  cacheName, flags, acquireRemoteLock, globalTransaction);
      this.minVersion = minVersion;
      this.maxVersion = maxVersion;
   }

   public void initializeGMUComponents(CommitLog commitLog, VersionGenerator versionGenerator, Configuration configuration) {
      this.commitLog = commitLog;
      this.configuration = configuration;
   }

   @Override
   protected InvocationContext createInvocationContext(GetKeyValueCommand command) {
      InvocationContext context = super.createInvocationContext(command);
      context.setVersionToRead(maxVersion);

      GMUEntryVersion minGMUVersion = toGMUEntryVersion(minVersion);
      GMUEntryVersion maxGMUVersion = toGMUEntryVersion(maxVersion);

      long timeout = configuration.getSyncReplTimeout() / 2;

      boolean alreadyReadOnThisNode = maxGMUVersion != null &&
            maxGMUVersion.getThisNodeVersionValue() == GMUEntryVersion.NON_EXISTING;
      context.setAlreadyReadOnThisNode(alreadyReadOnThisNode);

      if(!alreadyReadOnThisNode){
         if (minGMUVersion == null) {
            throw new NullPointerException("Min Version cannot be null");
         }
         try {
            if(!commitLog.waitForVersion(minGMUVersion, timeout)) {
               log.warnf("Receive remote get request, but the value wanted is not available. key: %s," +
                               "min version: %s, max version: %s", getKey(), minVersion, maxVersion);
               return null; //no version available
            }
         } catch (InterruptedException e) {
            return null;
         }
      }
      context.setVersionToRead(commitLog.getAvailableVersionLessThan(maxGMUVersion));
      return context;
   }

   @Override
   protected InternalCacheValue invoke(GetKeyValueCommand command, InvocationContext context) {
      if (context == null) {
         return null;
      }
      super.invoke(command, context);
      InternalGMUCacheEntry gmuCacheEntry = context.getKeysReadInCommand().get(getKey());
      return gmuCacheEntry == null ? null : gmuCacheEntry.toInternalCacheValue();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      Object[] original = super.getParameters();
      Object[] retVal = new Object[original.length + 3];
      System.arraycopy(original, 0, retVal, 0, original.length);
      int index = original.length;
      retVal[index++] = minVersion;
      retVal[index] = maxVersion;
      return retVal;
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      int index = args.length - 3;
      super.setParameters(commandId, args);
      minVersion = (EntryVersion) args[index++];
      maxVersion = (EntryVersion) args[index];
   }


   @Override
   public String toString() {
      return new StringBuilder()
            .append("GMUClusteredGetCommand{key=").append(getKey())
            .append(", flags=").append(getFlags())
            .append(", minVersion=").append(minVersion)
            .append(", maxVersion=").append(maxVersion)
            .append("}")
            .toString();
   }
}
