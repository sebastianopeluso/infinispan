package org.infinispan.interceptors;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import eu.cloudtm.rmi.statistics.stream_lib.AnalyticsBean;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.Map;

/**
 * Date: 12/20/11
 * Time: 6:46 PM
 *
 * @author pruivo
 */
@MBean(objectName = "StreamLibStatistics", description = "Show analytics for workload monitor")
public class StreamLibInterceptor extends JmxStatsCommandInterceptor {

    private AnalyticsBean analyticsBean;

    @Inject
    public void inject(AnalyticsBean analyticsBean) {
        this.analyticsBean = analyticsBean;
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        if(getStatisticsEnabled()) {
            analyticsBean.addGet(command.getKey(), false);
        }
        return invokeNextInterceptor(ctx, command);
    }

    @Override
    public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
        if(getStatisticsEnabled()) {
            analyticsBean.addPut(command.getKey(), false);
        }
        return invokeNextInterceptor(ctx, command);
    }

    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset Statistics (Statistics)")
    @Override
    public void resetStatistics() {

    }

    @ManagedOperation(description = "Set K for the top-K values")
    @Operation(displayName = "Set K")
    public void setTopKValue(int value) {
        analyticsBean.setCapacity(value);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most read remotely by this instance")
    @Operation(displayName = "Top Remote Read Keys")
    public Map<Object, Long> getRemoteTopGets() {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_GET);
    }

    @ManagedOperation(description = "Show the top n keys most read remotely by this instance")
    @Operation(displayName = "Top Remote Read Keys")
    public Map<Object, Long> getRemoteTopGets(int n) {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_GET, n);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most read locally by this instance")
    @Operation(displayName = "Top Local Read Keys")
    public Map<Object, Long> getLocalTopGets() {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_GET);
    }

    @ManagedOperation(description = "Show the top n keys most read locally by this instance")
    @Operation(displayName = "Top Local Read Keys")
    public Map<Object, Long> getLocalTopGets(int n) {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_GET, n);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most write remotely by this instance")
    @Operation(displayName = "Top Remote Write Keys")
    public Map<Object, Long> getRemoteTopPuts() {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_PUT);
    }

    @ManagedOperation(description = "Show the top n keys most write remotely by this instance")
    @Operation(displayName = "Top Remote Write Keys")
    public Map<Object, Long> getRemoteTopPuts(int n) {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_PUT, n);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most write locally by this instance")
    @Operation(displayName = "Top Local Write Keys")
    public Map<Object, Long> getLocalTopPuts() {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_PUT);
    }

    @ManagedOperation(description = "Show the top n keys most write locally by this instance")
    @Operation(displayName = "Top Local Write Keys")
    public Map<Object, Long> getLocalTopPuts(int n) {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_PUT, n);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most locked")
    @Operation(displayName = "Top Locked Keys")
    public Map<Object, Long> getTopLockedKeys() {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_LOCKED_KEYS);
    }

    @ManagedOperation(description = "Show the top n keys most locked")
    @Operation(displayName = "Top Locked Keys")
    public Map<Object, Long> getTopLockedKeys(int n) {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_LOCKED_KEYS, n);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most contended")
    @Operation(displayName = "Top Contended Keys")
    public Map<Object, Long> getTopContendedKeys() {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_CONTENDED_KEYS);
    }

    @ManagedOperation(description = "Show the top n keys most contended")
    @Operation(displayName = "Top Contended Keys")
    public Map<Object, Long> getTopContendedKeys(int n) {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_CONTENDED_KEYS, n);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys where the lock acquisition failed by timeout")
    @Operation(displayName = "Top Keys where Lock Acquisition Failed by Timeout")
    public Map<Object, Long> getTopLockFailedByTimeoutKeys() {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_FAILED_KEYS_BY_TIMEOUT);
    }

    @ManagedOperation(description = "Show the top n keys where the lock acquisition failed by timeout")
    @Operation(displayName = "Top Keys where Lock Acquisition Failed by Timeout")
    public Map<Object, Long> getTopLockFailedByTimeoutKeys(int n) {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_FAILED_KEYS_BY_TIMEOUT, n);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys where the lock acquisition failed by deadlock")
    @Operation(displayName = "Top Keys where Lock Acquisition Failed by Deadlock")
    public Map<Object, Long> getTopLockFailedByDeadlockKeys() {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_FAILED_KEYS_BY_DEADLOCK);
    }

    @ManagedOperation(description = "Show the top n keys where the lock acquisition failed by deadlock")
    @Operation(displayName = "Top Keys where Lock Acquisition Failed by Deadlock")
    public Map<Object, Long> getTopLockFailedByDeadlockKeys(int n) {
        return analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_FAILED_KEYS_BY_DEADLOCK, n);
    }
}
