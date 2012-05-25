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
package org.infinispan.remoting.transport.jgroups;

import org.infinispan.CacheConfigurationException;
import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.AbstractTransport;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.FileLookup;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Channel;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.MergeView;
import org.jgroups.View;
import org.jgroups.blocks.RspFilter;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.SEQUENCER;
import org.jgroups.protocols.tom.TOA;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.TopologyUUID;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.infinispan.factories.KnownComponentNames.GLOBAL_MARSHALLER;

/**
 * An encapsulation of a JGroups transport. JGroups transports can be configured using a variety of
 * methods, usually by passing in one of the following properties:
 * <ul>
 * <li><tt>configurationString</tt> - a JGroups configuration String</li>
 * <li><tt>configurationXml</tt> - JGroups configuration XML as a String</li>
 * <li><tt>configurationFile</tt> - String pointing to a JGroups XML configuration file</li>
 * <li><tt>channelLookup</tt> - Fully qualified class name of a
 * {@link org.infinispan.remoting.transport.jgroups.JGroupsChannelLookup} instance</li>
 * </ul>
 * These are normally passed in as Properties in
 * {@link org.infinispan.config.GlobalConfiguration#setTransportProperties(java.util.Properties)} or
 * in the Infinispan XML configuration file.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Pedro Ruivo
 * @since 4.0
 */
public class JGroupsTransport extends AbstractTransport implements MembershipListener {
   public static final String CONFIGURATION_STRING = "configurationString";
   public static final String CONFIGURATION_XML = "configurationXml";
   public static final String CONFIGURATION_FILE = "configurationFile";
   public static final String CHANNEL_LOOKUP = "channelLookup";
   protected static final String DEFAULT_JGROUPS_CONFIGURATION_FILE = "jgroups-udp.xml";

   static final Log log = LogFactory.getLog(JGroupsTransport.class);
   static final boolean trace = log.isTraceEnabled();
   final ConcurrentMap<String, StateTransferMonitor> stateTransfersInProgress = ConcurrentMapFactory.makeConcurrentMap();

   protected boolean startChannel = true, stopChannel = true;
   private CommandAwareRpcDispatcher dispatcher;
   protected TypedProperties props;
   protected InboundInvocationHandler inboundInvocationHandler;
   protected StreamingMarshaller marshaller;
   protected ExecutorService asyncExecutor;
   protected CacheManagerNotifier notifier;

   private boolean globalStatsEnabled;
   private MBeanServer mbeanServer;
   private String domain;

   protected Channel channel;
   protected Address address;
   protected Address physicalAddress;

   // these members are not valid until we have received the first view on a second thread
   // and channelConnectedLatch is signaled
   protected volatile List<Address> members = null;
   protected volatile Address coordinator = null;
   protected volatile boolean isCoordinator = false;
   protected CountDownLatch channelConnectedLatch = new CountDownLatch(1);

   /**
    * This form is used when the transport is created by an external source and passed in to the
    * GlobalConfiguration.
    *
    * @param channel
    *           created and running channel to use
    */
   public JGroupsTransport(Channel channel) {
      this.channel = channel;
      if (channel == null)
         throw new IllegalArgumentException("Cannot deal with a null channel!");
      if (channel.isConnected())
         throw new IllegalArgumentException("Channel passed in cannot already be connected!");
   }

   public JGroupsTransport() {
   }

   @Override
   public Log getLog() {
      return log;
   }

   // ------------------------------------------------------------------------------------------------------------------
   // Lifecycle and setup stuff
   // ------------------------------------------------------------------------------------------------------------------

   @Override
   public void initialize(@ComponentName(GLOBAL_MARSHALLER) StreamingMarshaller marshaller, ExecutorService asyncExecutor, InboundInvocationHandler inboundInvocationHandler,
            CacheManagerNotifier notifier) {
      this.marshaller = marshaller;
      this.asyncExecutor = asyncExecutor;
      this.inboundInvocationHandler = inboundInvocationHandler;
      this.notifier = notifier;
   }

   @Override
   public void start() {
      props = TypedProperties.toTypedProperties(configuration.getTransportProperties());

      if (log.isInfoEnabled())
         log.startingJGroupsChannel();

      initChannelAndRPCDispatcher();
      startJGroupsChannelIfNeeded();

      waitForChannelToConnect();
   }

   protected void startJGroupsChannelIfNeeded() {
      if (startChannel) {
         String clusterName = configuration.getClusterName();
         try {
            channel.connect(clusterName);
         } catch (Exception e) {
            throw new CacheException("Unable to start JGroups Channel", e);
         }

         try {
            // Normally this would be done by CacheManagerJmxRegistration but
            // the channel is not started when the cache manager starts but
            // when first cache starts, so it's safer to do it here.
            globalStatsEnabled = configuration.isExposeGlobalJmxStatistics();
            if (globalStatsEnabled) {
               String groupName = String.format("type=channel,cluster=%s", ObjectName.quote(clusterName));
               mbeanServer = JmxUtil.lookupMBeanServer(configuration);
               domain = JmxUtil.buildJmxDomain(configuration, mbeanServer, groupName);
               JmxConfigurator.registerChannel((JChannel) channel, mbeanServer, domain, clusterName, true);
            }
         } catch (Exception e) {
            throw new CacheException("Channel connected, but unable to register MBeans", e);
         }
      }
      address = fromJGroupsAddress(channel.getAddress());
      if (!startChannel) {
         // the channel was already started externally, we need to initialize our member list
         viewAccepted(channel.getView());
      }
      if (log.isInfoEnabled())
         log.localAndPhysicalAddress(getAddress(), getPhysicalAddresses());
   }

   @Override
   public int getViewId() {
      if (channel == null)
         throw new CacheException("The cache has been stopped and invocations are not allowed!");
      View view = channel.getView();
      if (view == null)
         return -1;
      return (int) view.getVid().getId();
   }

   @Override
   public void stop() {
      try {
         if (stopChannel && channel != null && channel.isOpen()) {
            log.disconnectAndCloseJGroups();

            // Unregistering before disconnecting/closing because
            // after that the cluster name is null
            if (globalStatsEnabled) {
               JmxConfigurator.unregisterChannel((JChannel) channel, mbeanServer, domain, channel.getClusterName());
            }

            channel.disconnect();
            channel.close();
         }
      } catch (Exception toLog) {
         log.problemClosingChannel(toLog);
      }

      channel = null;
      if (dispatcher != null) {
         log.stoppingRpcDispatcher();
         dispatcher.stop();
      }

      members = Collections.emptyList();
      coordinator = null;
      isCoordinator = false;
      dispatcher = null;
   }

   protected void initChannel() {
      if (channel == null) {
         buildChannel();
         String transportNodeName = configuration.getTransportNodeName();
         if (transportNodeName != null && transportNodeName.length() > 0) {
            long range = Short.MAX_VALUE * 2;
            long randomInRange = (long) ((Math.random() * range) % range) + 1;
            transportNodeName = transportNodeName + "-" + randomInRange;
            channel.setName(transportNodeName);
         }
      }

      // Channel.LOCAL *must* be set to false so we don't see our own messages - otherwise
      // invalidations targeted at remote instances will be received by self.
      // WARNING: total order needs to deliver own messages. the invokeRemotely method has a total order boolean
      //          that when it is false, it discard our own messages, maintaining the property needed
      channel.setDiscardOwnMessages(false);

      // if we have a TopologyAwareConsistentHash, we need to set our own address generator in
      // JGroups
      if (configuration.hasTopologyInfo()) {
         // We can do this only if the channel hasn't been started already
         if (startChannel) {
            ((JChannel) channel).setAddressGenerator(new AddressGenerator() {

               @Override
               public org.jgroups.Address generateAddress() {
                  return TopologyUUID.randomUUID(channel.getName(), configuration.getSiteId(), configuration.getRackId(), configuration.getMachineId());
               }
            });
         } else {
            if (channel.getAddress() instanceof TopologyUUID) {
               TopologyUUID topologyAddress = (TopologyUUID) channel.getAddress();
               if (!configuration.getSiteId().equals(topologyAddress.getSiteId()) || !configuration.getRackId().equals(topologyAddress.getRackId()) || !configuration.getMachineId().equals(topologyAddress.getMachineId())) {
                  throw new CacheException("Topology information does not match the one set by the provided JGroups channel");
               }
            } else {
               throw new CacheException("JGroups address does not contain topology coordinates");
            }
         }
      }
   }

   private void initChannelAndRPCDispatcher() throws CacheException {
      initChannel();
      dispatcher = new CommandAwareRpcDispatcher(channel, this, asyncExecutor, inboundInvocationHandler);
      MarshallerAdapter adapter = new MarshallerAdapter(marshaller);
      dispatcher.setRequestMarshaller(adapter);
      dispatcher.setResponseMarshaller(adapter);
      dispatcher.start();
   }

   // This is per CM, so the CL in use should be the CM CL
   private void buildChannel() {
      // in order of preference - we first look for an external JGroups file, then a set of XML
      // properties, and
      // finally the legacy JGroups String properties.
      String cfg;
      if (props != null) {
         if (props.containsKey(CHANNEL_LOOKUP)) {
            String channelLookupClassName = props.getProperty(CHANNEL_LOOKUP);

            try {
               JGroupsChannelLookup lookup = (JGroupsChannelLookup) Util.getInstance(channelLookupClassName, configuration.getClassLoader());
               channel = lookup.getJGroupsChannel(props);
               startChannel = lookup.shouldStartAndConnect();
               stopChannel = lookup.shouldStopAndDisconnect();
            } catch (ClassCastException e) {
               log.wrongTypeForJGroupsChannelLookup(channelLookupClassName, e);
               throw new CacheException(e);
            } catch (Exception e) {
               log.errorInstantiatingJGroupsChannelLookup(channelLookupClassName, e);
               throw new CacheException(e);
            }
            if (configuration.isStrictPeerToPeer() && !startChannel) {
               log.warnStrictPeerToPeerWithInjectedChannel();
            }
         }

         if (channel == null && props.containsKey(CONFIGURATION_FILE)) {
            cfg = props.getProperty(CONFIGURATION_FILE);
            URL conf = FileLookupFactory.newInstance().lookupFileLocation(cfg, configuration.getClassLoader());
            if (conf == null) {
               throw new CacheConfigurationException(CONFIGURATION_FILE
                        + " property specifies value " + conf + " that could not be read!",
                        new FileNotFoundException(cfg));
            }
            try {
               channel = new JChannel(conf);
            } catch (Exception e) {
               log.errorCreatingChannelFromConfigFile(cfg);
               throw new CacheException(e);
            }
         }

         if (channel == null && props.containsKey(CONFIGURATION_XML)) {
            cfg = props.getProperty(CONFIGURATION_XML);
            try {
               channel = new JChannel(XmlConfigHelper.stringToElement(cfg));
            } catch (Exception e) {
               log.errorCreatingChannelFromXML(cfg);
               throw new CacheException(e);
            }
         }

         if (channel == null && props.containsKey(CONFIGURATION_STRING)) {
            cfg = props.getProperty(CONFIGURATION_STRING);
            try {
               channel = new JChannel(cfg);
            } catch (Exception e) {
               log.errorCreatingChannelFromConfigString(cfg);
               throw new CacheException(e);
            }
         }
      }

      if (channel == null) {
         log.unableToUseJGroupsPropertiesProvided(props);
         try {
            channel = new JChannel(FileLookupFactory.newInstance().lookupFileLocation(DEFAULT_JGROUPS_CONFIGURATION_FILE, configuration.getClassLoader()));
         } catch (Exception e) {
            throw new CacheException("Unable to start JGroups channel", e);
         }
      }
   }

   // ------------------------------------------------------------------------------------------------------------------
   // querying cluster status
   // ------------------------------------------------------------------------------------------------------------------

   @Override
   public boolean isCoordinator() {
      return isCoordinator;
   }

   @Override
   public Address getCoordinator() {
      return coordinator;
   }

   public void waitForChannelToConnect() {
      if (channel == null)
         return;
      log.debug("Waiting on view being accepted");
      try {
         channelConnectedLatch.await();
      } catch (InterruptedException e) {
         log.interruptedWaitingForCoordinator(e);
      }
   }

   @Override
   public List<Address> getMembers() {
      return members != null ? members : Collections.<Address> emptyList();
   }

   @Override
   public boolean isMulticastCapable() {
      return channel.getProtocolStack().getTransport().supportsMulticasting();
   }

   @Override
   public Address getAddress() {
      if (address == null && channel != null) {
         address = fromJGroupsAddress(channel.getAddress());
      }
      return address;
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      if (physicalAddress == null && channel != null) {
         org.jgroups.Address addr = (org.jgroups.Address) channel.down(new Event(Event.GET_PHYSICAL_ADDRESS, channel.getAddress()));
         if (addr == null) {
            return Collections.emptyList();
         }
         physicalAddress = new JGroupsAddress(addr);
      }
      return Collections.singletonList(physicalAddress);
   }

   // ------------------------------------------------------------------------------------------------------------------
   // outbound RPC
   // ------------------------------------------------------------------------------------------------------------------

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter,
                                                boolean totalOrder, boolean distribution) throws Exception {

      if (recipients != null && recipients.isEmpty()) {
         // don't send if dest list is empty
         log.trace("Destination list is empty: no need to send message");
         return Collections.emptyMap();
      }

      if (trace)
         log.tracef("dests=%s, command=%s, mode=%s, timeout=%s", recipients, rpcCommand, mode, timeout);
      Address self = getAddress();
      if (mode.isSynchronous() && recipients != null && !getMembers().containsAll(recipients)) {
         if (mode == ResponseMode.SYNCHRONOUS)
            throw new SuspectException("One or more nodes have left the cluster while replicating command " + rpcCommand);
         else { // SYNCHRONOUS_IGNORE_LEAVERS || WAIT_FOR_VALID_RESPONSE
            recipients = new HashSet<Address>(recipients);
            recipients.retainAll(getMembers());
         }
      }
      boolean asyncMarshalling = mode == ResponseMode.ASYNCHRONOUS;
      if (!usePriorityQueue && (ResponseMode.SYNCHRONOUS == mode || ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS == mode))
         usePriorityQueue = true;

      List<org.jgroups.Address> jgAddressList = toJGroupsAddressListExcludingSelf(recipients);
      int membersSize = members.size();
      boolean broadcast = jgAddressList == null || recipients.size() == membersSize;
      if (membersSize < 3 || (jgAddressList != null && jgAddressList.size() < 2)) {
         broadcast = false;
      }
      RspList<Object> rsps = null;
      Response singleResponse = null;
      org.jgroups.Address singleJGAddress = null;

      if (broadcast || (totalOrder && !distribution)) {
         rsps = dispatcher.broadcastRemoteCommands(rpcCommand, toJGroupsMode(mode), timeout, recipients != null,
                                                   usePriorityQueue, toJGroupsFilter(responseFilter),
                                                   asyncMarshalling, totalOrder, distribution);
      } else if (totalOrder && distribution) {
         rsps = dispatcher.invokeRemoteCommands(jgAddressList, rpcCommand, toJGroupsMode(mode), timeout,
                                                recipients != null, usePriorityQueue, toJGroupsFilter(responseFilter),
                                                asyncMarshalling, totalOrder, distribution);
      } else if (jgAddressList == null || !jgAddressList.isEmpty()) {
         boolean singleRecipient = jgAddressList != null && jgAddressList.size() == 1;
         boolean skipRpc = false;
         if (jgAddressList == null) {
            ArrayList<Address> others = new ArrayList<Address>(members);
            others.remove(self);
            skipRpc = others.isEmpty();
            singleRecipient = others.size() == 1;
            if (singleRecipient) singleJGAddress = toJGroupsAddress(others.get(0));
         }
         if (!skipRpc) {
            if (singleRecipient && !totalOrder) {
               if (singleJGAddress == null) singleJGAddress = jgAddressList.get(0);
               singleResponse = dispatcher.invokeRemoteCommand(singleJGAddress, rpcCommand, toJGroupsMode(mode), timeout,
                                                               usePriorityQueue, asyncMarshalling);
            } else {
               rsps = dispatcher.invokeRemoteCommands(jgAddressList, rpcCommand, toJGroupsMode(mode), timeout,
                                                      recipients != null, usePriorityQueue, toJGroupsFilter(responseFilter),
                                                      asyncMarshalling, totalOrder, distribution);
            }
         }
      }

      if (mode.isAsynchronous())
         return Collections.emptyMap();// async case

      Map<Address, Response> responses;
      if (rsps == null) {
         if (singleJGAddress == null || (singleResponse == null && rpcCommand instanceof ClusteredGetCommand)) {
            responses = Collections.emptyMap();
         } else {
            responses = Collections.singletonMap(fromJGroupsAddress(singleJGAddress), singleResponse);
         }
      } else {
         Map<Address, Response> retval = new HashMap<Address, Response>(rsps.size());

         boolean ignoreLeavers = mode == ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS || mode == ResponseMode.WAIT_FOR_VALID_RESPONSE;
         boolean noValidResponses = true;
         for (Rsp<Object> rsp : rsps.values()) {
            noValidResponses &= parseResponseAndAddToResponseList(rsp.getValue(), rsp.getException(), retval, rsp.wasSuspected(), rsp.wasReceived(), fromJGroupsAddress(rsp.getSender()),
                  responseFilter != null, ignoreLeavers);
         }

         if (noValidResponses)
            throw new TimeoutException("Timed out waiting for valid responses!");
         responses = retval;
      }
      return responses;
   }

   private static org.jgroups.blocks.ResponseMode toJGroupsMode(ResponseMode mode) {
      switch (mode) {
         case ASYNCHRONOUS:
         case ASYNCHRONOUS_WITH_SYNC_MARSHALLING:
            return org.jgroups.blocks.ResponseMode.GET_NONE;
         case SYNCHRONOUS:
         case SYNCHRONOUS_IGNORE_LEAVERS:
            return org.jgroups.blocks.ResponseMode.GET_ALL;
         case WAIT_FOR_VALID_RESPONSE:
            return org.jgroups.blocks.ResponseMode.GET_FIRST;
      }
      throw new CacheException("Unknown response mode " + mode);
   }

   private RspFilter toJGroupsFilter(ResponseFilter responseFilter) {
      return responseFilter == null ? null : new JGroupsResponseFilterAdapter(responseFilter);
   }

   // ------------------------------------------------------------------------------------------------------------------
   // Implementations of JGroups interfaces
   // ------------------------------------------------------------------------------------------------------------------

   private interface Notify {
      void emitNotification(List<Address> oldMembers, View newView);
   }

   private class NotifyViewChange implements Notify {
      @Override
      public void emitNotification(List<Address> oldMembers, View newView) {
         notifier.notifyViewChange(members, oldMembers, getAddress(), (int) newView.getVid().getId());
      }
   }

   private class NotifyMerge implements Notify {

      @Override
      public void emitNotification(List<Address> oldMembers, View newView) {
         MergeView mv = (MergeView) newView;

         final Address address = getAddress();
         final int viewId = (int) newView.getVid().getId();
         notifier.notifyMerge(members, oldMembers, address, viewId, getSubgroups(mv.getSubgroups()));
      }

      private List<List<Address>> getSubgroups(List<View> subviews) {
         List<List<Address>> l = new ArrayList<List<Address>>(subviews.size());
         for (View v : subviews)
            l.add(fromJGroupsAddressList(v.getMembers()));
         return l;
      }
   }

   @Override
   public void viewAccepted(View newView) {
      log.debugf("New view accepted: %s", newView);
      List<org.jgroups.Address> newMembers = newView.getMembers();
      if (newMembers == null || newMembers.isEmpty()) {
         log.debugf("Received null or empty member list from JGroups channel: " + newView);
         return;
      }

      List<Address> oldMembers = members;
      // we need a defensive copy anyway
      members = fromJGroupsAddressList(newMembers);

      // Now that we have a view, figure out if we are the isCoordinator
      coordinator = fromJGroupsAddress(newView.getCreator());
      isCoordinator = coordinator != null && coordinator.equals(getAddress());

      // Wake up any threads that are waiting to know about who the isCoordinator is
      // do it before the notifications, so if a listener throws an exception we can still start
      channelConnectedLatch.countDown();

      // now notify listeners - *after* updating the isCoordinator. - JBCACHE-662
      boolean hasNotifier = notifier != null;
      if (hasNotifier) {
         Notify n;
         if (newView instanceof MergeView) {
            log.receivedMergedView(newView);
            n = new NotifyMerge();
         } else {
            log.receivedClusterView(newView);
            n = new NotifyViewChange();
         }

         n.emitNotification(oldMembers, newView);
      }
   }

   @Override
   public void suspect(org.jgroups.Address suspected_mbr) {
      // no-op
   }

   @Override
   public void block() {
      // no-op since ISPN-83 has been resolved
   }

   @Override
   public void unblock() {
      // no-op since ISPN-83 has been resolved
   }

   // ------------------------------------------------------------------------------------------------------------------
   // Helpers to convert between Address types
   // ------------------------------------------------------------------------------------------------------------------

   protected static org.jgroups.Address toJGroupsAddress(Address a) {
      return ((JGroupsAddress) a).address;
   }

   static Address fromJGroupsAddress(org.jgroups.Address addr) {
      if (addr instanceof TopologyUUID)
         return new JGroupsTopologyAwareAddress((TopologyUUID)addr);
      else
         return new JGroupsAddress(addr);
   }

   private List<org.jgroups.Address> toJGroupsAddressListExcludingSelf(Collection<Address> list) {
      if (list == null)
         return null;
      if (list.isEmpty())
         return Collections.emptyList();

      List<org.jgroups.Address> retval = new ArrayList<org.jgroups.Address>(list.size());
      boolean ignoreSelf = true;
      Address self = getAddress();
      for (Address a : list) {
         if (!ignoreSelf || !a.equals(self)) {
            retval.add(toJGroupsAddress(a));
         } else {
            ignoreSelf = false; // short circuit address equality for future iterations
         }
      }

      return retval;
   }

   private static List<Address> fromJGroupsAddressList(List<org.jgroups.Address> list) {
      if (list == null || list.isEmpty())
         return Collections.emptyList();

      List<Address> retval = new ArrayList<Address>(list.size());
      for (org.jgroups.Address a : list)
         retval.add(fromJGroupsAddress(a));
      return Collections.unmodifiableList(retval);
   }

   // mainly for unit testing

   public CommandAwareRpcDispatcher getCommandAwareRpcDispatcher() {
      return dispatcher;
   }

   public Channel getChannel() {
      return channel;
   }

   @Override
   public final void checkTotalOrderSupported(boolean distributed) {
      if (!distributed && channel.getProtocolStack().findProtocol(SEQUENCER.class) == null)  {
         throw new ConfigurationException("In order to support total order based transaction, the SEQUENCER protocol " +
                                                "must be present in the JGroups's config.");
      } else if (distributed && channel.getProtocolStack().findProtocol(TOA.class) == null) {
         throw new ConfigurationException("In order to support total order based transaction, the TOA protocol " +
                                                "must be present in the JGroups's config.");
      }
   }
}
