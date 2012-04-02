package org.infinispan.interceptors;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.stats.StreamLibContainer;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.Map;

/**
 * Collects stats about the access pattern, i.e., the most access keys for this cache, the most locked keys, etc.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "StreamLibStatistics", description = "Show top-key stats")
public class StreamLibInterceptor extends JmxStatsCommandInterceptor {

   private StreamLibContainer streamLibContainer;

   @Inject
   public void inject(StreamLibContainer streamLibContainer) {
      this.streamLibContainer = streamLibContainer;
   }

   protected boolean isRemote(Object k){
      return false;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {

      if(getStatisticsEnabled() && ctx.isInTxScope() && ctx.isOriginLocal()) {
         streamLibContainer.addGet(command.getKey(), isRemote(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if(getStatisticsEnabled() && ctx.isInTxScope() && ctx.isOriginLocal()) {
         streamLibContainer.addPut(command.getKey(), isRemote(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset Statistics (Statistics)")
   @Override
   public void resetStatistics() {
      streamLibContainer.resetAll();

   }

   @ManagedOperation(description = "Set K for the top-K values")
   @Operation(displayName = "Set K")
   public void setTopKValue(int value) {
      streamLibContainer.setCapacity(value);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most read remotely by this instance")
   @Operation(displayName = "Top Remote Read Keys")
   public Map<Object, Long> getRemoteTopGets() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET);
   }

   @ManagedOperation(description = "Show the top n keys most read remotely by this instance")
   @Operation(displayName = "Top Remote Read Keys")
   public Map<Object, Long> getNRemoteTopGets(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.REMOTE_GET);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most read locally by this instance")
   @Operation(displayName = "Top Local Read Keys")
   public Map<Object, Long> getLocalTopGets() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET);
   }

   @ManagedOperation(description = "Show the top n keys most read locally by this instance")
   @Operation(displayName = "Top Local Read Keys")
   public Map<Object, Long> getNLocalTopGets(int n) {
      Map<Object, Long> res =  streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.LOCAL_GET);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most write remotely by this instance")
   @Operation(displayName = "Top Remote Write Keys")
   public Map<Object, Long> getRemoteTopPuts() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_PUT);
   }

   @ManagedOperation(description = "Show the top n keys most write remotely by this instance")
   @Operation(displayName = "Top Remote Write Keys")
   public Map<Object, Long> getNRemoteTopPuts(int n) {
      Map<Object, Long> res =  streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_PUT, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.REMOTE_PUT);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most write locally by this instance")
   @Operation(displayName = "Top Local Write Keys")
   public Map<Object, Long> getLocalTopPuts() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_PUT);
   }

   @ManagedOperation(description = "Show the top n keys most write locally by this instance")
   @Operation(displayName = "Top Local Write Keys")
   public Map<Object, Long> getNLocalTopPuts(int n) {
      Map<Object, Long> res  = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_PUT, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.LOCAL_PUT);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most locked")
   @Operation(displayName = "Top Locked Keys")
   public Map<Object, Long> getTopLockedKeys() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_LOCKED_KEYS);
   }

   @ManagedOperation(description = "Show the top n keys most locked")
   @Operation(displayName = "Top Locked Keys")
   public Map<Object, Long> getNTopLockedKeys(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_LOCKED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_LOCKED_KEYS);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most contended")
   @Operation(displayName = "Top Contended Keys")
   public Map<Object, Long> getTopContendedKeys() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_CONTENDED_KEYS);
   }

   @ManagedOperation(description = "Show the top n keys most contended")
   @Operation(displayName = "Top Contended Keys")
   public Map<Object, Long> getNTopContendedKeys(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_CONTENDED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_CONTENDED_KEYS);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys whose lock acquisition failed by timeout")
   @Operation(displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<Object, Long> getTopLockFailedKeys() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_FAILED_KEYS);
   }

   @ManagedOperation(description = "Show the top n keys whose lock acquisition failed ")
   @Operation(displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<Object, Long> getNTopLockFailedKeys(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_FAILED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_FAILED_KEYS);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys whose write skew check was failed")
   @Operation(displayName = "Top Keys whose Write Skew Check was failed")
   public Map<Object, Long> getTopWriteSkewFailedKeys() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS);
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed")
   @Operation(displayName = "Top Keys whose Write Skew Check was failed")
   public Map<Object, Long> getNTopWriteSkewFailedKeys(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS);
      return res;
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      super.setStatisticsEnabled(enabled);
      streamLibContainer.setActive(enabled);
   }
}
