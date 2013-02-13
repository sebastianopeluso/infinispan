package org.infinispan.stats.topK;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.WriteSkewException;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "StreamLibStatistics", description = "Show analytics for workload monitor")
public abstract class StreamSummaryInterceptor extends BaseCustomInterceptor {

   private StreamLibContainer streamLibContainer;

   @Override
   protected void start() {
      super.start();
      streamLibContainer = StreamSummaryManager.getStreamLibForCache(cache.getName());
      streamLibContainer.setActive(true);
   }

   protected abstract boolean isRemote(Object key);

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {

      if (streamLibContainer.isActive() && ctx.isOriginLocal() && ctx.isInTxScope()) {
         streamLibContainer.addGet(command.getKey(), isRemote(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         if (streamLibContainer.isActive() && ctx.isOriginLocal() && ctx.isInTxScope()) {
            streamLibContainer.addPut(command.getKey(), isRemote(command.getKey()));
         }
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset Statistics (Statistics)")
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
   public Map<String, Long> getRemoteTopGets() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET));
   }

   @ManagedOperation(description = "Show the top n keys most read remotely by this instance")
   @Operation(displayName = "Top Remote Read Keys")
   public Map<String, Long> getNRemoteTopGets(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.REMOTE_GET);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most read locally by this instance")
   @Operation(displayName = "Top Local Read Keys")
   public Map<String, Long> getLocalTopGets() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET));
   }

   @ManagedOperation(description = "Show the top n keys most read locally by this instance")
   @Operation(displayName = "Top Local Read Keys")
   public Map<String, Long> getNLocalTopGets(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.LOCAL_GET);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most write remotely by this instance")
   @Operation(displayName = "Top Remote Write Keys")
   public Map<String, Long> getRemoteTopPuts() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_PUT));
   }

   @ManagedOperation(description = "Show the top n keys most write remotely by this instance")
   @Operation(displayName = "Top Remote Write Keys")
   public Map<String, Long> getNRemoteTopPuts(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_PUT, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.REMOTE_PUT);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most write locally by this instance")
   @Operation(displayName = "Top Local Write Keys")
   public Map<String, Long> getLocalTopPuts() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_PUT));
   }

   @ManagedOperation(description = "Show the top n keys most write locally by this instance")
   @Operation(displayName = "Top Local Write Keys")
   public Map<String, Long> getNLocalTopPuts(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_PUT, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.LOCAL_PUT);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most locked")
   @Operation(displayName = "Top Locked Keys")
   public Map<String, Long> getTopLockedKeys() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_LOCKED_KEYS));
   }

   @ManagedOperation(description = "Show the top n keys most locked")
   @Operation(displayName = "Top Locked Keys")
   public Map<String, Long> getNTopLockedKeys(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_LOCKED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_LOCKED_KEYS);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most contended")
   @Operation(displayName = "Top Contended Keys")
   public Map<String, Long> getTopContendedKeys() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_CONTENDED_KEYS));
   }

   @ManagedOperation(description = "Show the top n keys most contended")
   @Operation(displayName = "Top Contended Keys")
   public Map<String, Long> getNTopContendedKeys(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_CONTENDED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_CONTENDED_KEYS);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys whose lock acquisition failed by timeout")
   @Operation(displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<String, Long> getTopLockFailedKeys() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_FAILED_KEYS));
   }

   @ManagedOperation(description = "Show the top n keys whose lock acquisition failed ")
   @Operation(displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<String, Long> getNTopLockFailedKeys(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_FAILED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_FAILED_KEYS);
      return convertAndSort(res);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys whose write skew check was failed")
   @Operation(displayName = "Top Keys whose Write Skew Check was failed")
   public Map<String, Long> getTopWriteSkewFailedKeys() {
      return convertAndSort(streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS));
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed")
   @Operation(displayName = "Top Keys whose Write Skew Check was failed")
   public Map<String, Long> getNTopWriteSkewFailedKeys(int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS);
      return convertAndSort(res);
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed")
   @Operation(displayName = "Top Keys whose Write Skew Check was failed")
   public void setStatisticsEnabled(boolean enabled) {
      streamLibContainer.setActive(enabled);
   }

   private Map<String, Long> convertAndSort(Map<Object, Long> topKeyMap) {
      Map<String, Long> sorted = new LinkedHashMap<String, Long>();
      TopKeyEntry[] array = new TopKeyEntry[topKeyMap.size()];
      int insertPosition = 0;
      for (Map.Entry<Object, Long> entry : topKeyMap.entrySet()) {
         array[insertPosition++] = new TopKeyEntry(entry.getKey(), entry.getValue());
      }
      Arrays.sort(array);
      for (TopKeyEntry topKeyEntry : array) {
         sorted.put(String.valueOf(topKeyEntry.key), topKeyEntry.value);
      }
      return sorted;
   }

   private class TopKeyEntry implements Comparable<TopKeyEntry> {

      private final Object key;
      private final long value;

      private TopKeyEntry(Object key, long value) {
         this.key = key;
         this.value = value;
      }

      @Override
      public int compareTo(TopKeyEntry topKeyEntry) {
         return topKeyEntry == null ? 1 : Long.valueOf(value).compareTo(topKeyEntry.value);
      }
   }


}
