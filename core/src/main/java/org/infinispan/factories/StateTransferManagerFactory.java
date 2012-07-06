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
package org.infinispan.factories;

import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.reconfigurableprotocol.component.StateTransferManagerDelegate;
import org.infinispan.reconfigurableprotocol.exception.AlreadyExistingComponentProtocolException;
import org.infinispan.reconfigurableprotocol.protocol.PassiveReplicationCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TotalOrderCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TwoPhaseCommitProtocol;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.DummyInvalidationStateTransferManagerImpl;
import org.infinispan.statetransfer.ReplicatedStateTransferManagerImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.statetransfer.totalorder.TotalOrderDistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.totalorder.TotalOrderReplicatedStateTransferManagerImpl;

/**
 * Constructs {@link org.infinispan.statetransfer.StateTransferManager} instances.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @author Pedro Ruivo
 * @since 4.0
 */
@DefaultFactoryFor(classes = StateTransferManager.class)
public class StateTransferManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.getCacheMode().isClustered())
         return null;

      if (configuration.getCacheMode().isDistributed()) {
         componentRegistry.registerComponent(new DistributedStateTransferManagerImpl(),
                                             DistributedStateTransferManagerImpl.class);
         componentRegistry.registerComponent(new TotalOrderDistributedStateTransferManagerImpl(),
                                             TotalOrderDistributedStateTransferManagerImpl.class);

         StateTransferManagerDelegate stm = new StateTransferManagerDelegate();

         try {
            stm.add(TwoPhaseCommitProtocol.UID,
                    componentRegistry.getComponent(DistributedStateTransferManagerImpl.class));
            stm.add(PassiveReplicationCommitProtocol.UID,
                    componentRegistry.getComponent(DistributedStateTransferManagerImpl.class));
            stm.add(TotalOrderCommitProtocol.UID,
                    componentRegistry.getComponent(TotalOrderDistributedStateTransferManagerImpl.class));
         } catch (AlreadyExistingComponentProtocolException e) {
            throw new ConfigurationException(e);
         }

         return componentType.cast(stm);
      }else if (configuration.getCacheMode().isReplicated()) {
         componentRegistry.registerComponent(new ReplicatedStateTransferManagerImpl(),
                                             ReplicatedStateTransferManagerImpl.class);
         componentRegistry.registerComponent(new TotalOrderReplicatedStateTransferManagerImpl(),
                                             TotalOrderReplicatedStateTransferManagerImpl.class);

         StateTransferManagerDelegate stm = new StateTransferManagerDelegate();

         try {
            stm.add(TwoPhaseCommitProtocol.UID,
                    componentRegistry.getComponent(ReplicatedStateTransferManagerImpl.class));
            stm.add(PassiveReplicationCommitProtocol.UID,
                    componentRegistry.getComponent(ReplicatedStateTransferManagerImpl.class));
            stm.add(TotalOrderCommitProtocol.UID,
                    componentRegistry.getComponent(TotalOrderReplicatedStateTransferManagerImpl.class));
         } catch (AlreadyExistingComponentProtocolException e) {
            throw new ConfigurationException(e);
         }

         return componentType.cast(stm);
      } else if (configuration.getCacheMode().isInvalidation())
         return componentType.cast(new DummyInvalidationStateTransferManagerImpl());
      else
         return null;
   }
}
