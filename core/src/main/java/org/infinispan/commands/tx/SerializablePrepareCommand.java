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
package org.infinispan.commands.tx;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author Pedro Ruivo 
 * @since 5.2
 */
public class SerializablePrepareCommand extends PrepareCommand {

   private static final Log log = LogFactory.getLog(SerializablePrepareCommand.class);

   public static final byte COMMAND_ID = 101;

   private static final Object[] EMPTY_READ_SET_ARRAY = new Object[0];

   private Object[] readSet;
   private VersionVC version;
   //We don't want to serialize this factory
   private transient VersionVCFactory versionVCFactory;

   public SerializablePrepareCommand(String cacheName, GlobalTransaction gtx, boolean onePhaseCommit, WriteCommand... modifications) {
      super(cacheName, gtx, onePhaseCommit, modifications);
   }

   public SerializablePrepareCommand(String cacheName, GlobalTransaction gtx, List<WriteCommand> commands, boolean onePhaseCommit) {
      super(cacheName, gtx, commands, onePhaseCommit);
   }

   public SerializablePrepareCommand(String cacheName) {
      super(cacheName);
   }

   public void initialize(CacheNotifier notifier, RecoveryManager recoveryManager, VersionVCFactory versionVCFactory) {
      super.initialize(notifier, recoveryManager);
      this.versionVCFactory = versionVCFactory;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true; //we need the version from the other guys...
   }

   @Override
   public Object[] getParameters() {
      int numMods = modifications == null ? 0 : modifications.length;
      int i = 0;
      final int params = 5;
      int numReads = readSet == null ? 0 : readSet.length;
      Object[] retVal = new Object[numMods + numReads + params];
      retVal[i++] = globalTx;
      retVal[i++] = onePhaseCommit;
      retVal[i++] = version;
      retVal[i++] = numMods;
      retVal[i] = numReads;
      if (numMods > 0) {
         System.arraycopy(modifications, 0, retVal, params, numMods);
      }
      if (numReads > 0) {
         System.arraycopy(readSet, 0, retVal, params + numMods, numReads);
      }
      return retVal;
   }

   @Override
   @SuppressWarnings({"unchecked", "SuspiciousSystemArraycopy"})
   public void setParameters(int commandId, Object[] args) {
      int i = 0;
      globalTx = (GlobalTransaction) args[i++];
      onePhaseCommit = (Boolean) args[i++];
      version = (VersionVC) args[i++];
      int numMods = (Integer) args[i++];
      int numReads = (Integer) args[i++];
      if (numMods > 0) {
         modifications = new WriteCommand[numMods];
         System.arraycopy(args, i, modifications, 0, numMods);
      }
      if(numReads > 0){
         readSet = new Object[numReads];
         System.arraycopy(args, i + numMods, readSet, 0, numReads);
      }
   }

   public SerializablePrepareCommand copy() {
      SerializablePrepareCommand copy = new SerializablePrepareCommand(cacheName);
      copy.globalTx = globalTx;
      copy.modifications = modifications == null ? null : modifications.clone();
      copy.onePhaseCommit = onePhaseCommit;
      if(readSet != null){
         copy.readSet = new Object[readSet.length];
         System.arraycopy(readSet, 0, copy.readSet, 0, readSet.length);
      } else{
         copy.readSet = null;
      }

      try {
         copy.version = version != null ? version.clone() : null;
      } catch (CloneNotSupportedException e) {
         log.warnf("Exception caught when cloning versions. " + e.getMessage());
         log.trace(e);
         copy.version=null;
      }
      return copy;
   }

   @Override
   public String toString() {
      return "SerializablePrepareCommand {" +
            "modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
            ", onePhaseCommit=" + onePhaseCommit +
            ", readSet=" + (readSet == null ? null : Arrays.asList(readSet)) +
            ", gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }

   public Object[] getReadSet() {
      if(readSet == null) {
         return EMPTY_READ_SET_ARRAY;
      }
      Object[] result = new Object[readSet.length];
      System.arraycopy(readSet, 0, result, 0, readSet.length);
      return result;
   }

   public VersionVC getVersion() {
      return version != null ? version : this.versionVCFactory.createVersionVC();
   }

   public void setReadSet(Object[] readSet) {
      this.readSet = readSet;
   }

   public void setVersion(VersionVC version) {
      this.version = version;
   }
}
