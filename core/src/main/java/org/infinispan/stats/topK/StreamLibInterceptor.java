package org.infinispan.stats.topK;

import org.infinispan.factories.annotations.Start;
import org.infinispan.stats.topK.AnalyticsBean;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.stats.topK.AnalyticsBean;
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

   /*
   I comment out the injection to change as less files as possible
    @Inject
    public void inject(AnalyticsBean analyticsBean) {
        this.analyticsBean = analyticsBean;
    }
    */

   @Start
   public void init(){
      this.analyticsBean = AnalyticsBean.getInstance();
   }

    protected boolean isRemote(Object k){
        return false;
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {

        if(getStatisticsEnabled()) {
            analyticsBean.addGet(command.getKey(), isRemote(command.getKey()));
        }
        return invokeNextInterceptor(ctx, command);
    }

    @Override
    public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
        if(getStatisticsEnabled()) {
            analyticsBean.addPut(command.getKey(), isRemote(command.getKey()));
        }
        return invokeNextInterceptor(ctx, command);
    }

    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset ExposedStatistics (ExposedStatistics)")
    @Override
    public void resetStatistics() {
        this.analyticsBean.resetAll();

    }

    @ManagedOperation(description = "Set K for the top-K values")
    @Operation(displayName = "Set K")
    public void setTopKValue(int value) {
        analyticsBean.setCapacity(value);
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most read remotely by this instance")
    @Operation(displayName = "Top Remote Read Keys")
    public Map<Object, Long> getRemoteTopGets() {
        Map<Object, Long> res =  analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_GET);
        //analyticsBean.resetStat(AnalyticsBean.Stat.REMOTE_GET);
        return res;
    }

    @ManagedOperation(description = "Show the top n keys most read remotely by this instance")
    @Operation(displayName = "Top Remote Read Keys")
    public Map<Object, Long> getRemoteTopGets(int n) {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_GET, n);
        analyticsBean.resetStat(AnalyticsBean.Stat.REMOTE_GET);
        return res;
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most read locally by this instance")
    @Operation(displayName = "Top Local Read Keys")
    public Map<Object, Long> getLocalTopGets() {
        Map<Object, Long> res =  analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_GET);
        //analyticsBean.resetStat(AnalyticsBean.Stat.LOCAL_GET);
        return res;
    }

    @ManagedOperation(description = "Show the top n keys most read locally by this instance")
    @Operation(displayName = "Top Local Read Keys")
    public Map<Object, Long> getLocalTopGets(int n) {
        Map<Object, Long> res =  analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_GET, n);
        analyticsBean.resetStat(AnalyticsBean.Stat.LOCAL_GET);
        return res;
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most write remotely by this instance")
    @Operation(displayName = "Top Remote Write Keys")
    public Map<Object, Long> getRemoteTopPuts() {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_PUT);
        //analyticsBean.resetStat(AnalyticsBean.Stat.REMOTE_PUT);
        return res;
    }

    @ManagedOperation(description = "Show the top n keys most write remotely by this instance")
    @Operation(displayName = "Top Remote Write Keys")
    public Map<Object, Long> getRemoteTopPuts(int n) {
        Map<Object, Long> res =  analyticsBean.getTopKFrom(AnalyticsBean.Stat.REMOTE_PUT, n);
        analyticsBean.resetStat(AnalyticsBean.Stat.REMOTE_PUT);
        return res;
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most write locally by this instance")
    @Operation(displayName = "Top Local Write Keys")
    public Map<Object, Long> getLocalTopPuts() {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_PUT);
        //analyticsBean.resetStat(AnalyticsBean.Stat.LOCAL_PUT);
        return res;
    }

    @ManagedOperation(description = "Show the top n keys most write locally by this instance")
    @Operation(displayName = "Top Local Write Keys")
    public Map<Object, Long> getLocalTopPuts(int n) {
        Map<Object, Long> res  = analyticsBean.getTopKFrom(AnalyticsBean.Stat.LOCAL_PUT, n);
        analyticsBean.resetStat(AnalyticsBean.Stat.LOCAL_PUT);
        return res;
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most locked")
    @Operation(displayName = "Top Locked Keys")
    public Map<Object, Long> getTopLockedKeys() {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_LOCKED_KEYS);
        //analyticsBean.resetStat(AnalyticsBean.Stat.MOST_LOCKED_KEYS);
        return res;
    }

    @ManagedOperation(description = "Show the top n keys most locked")
    @Operation(displayName = "Top Locked Keys")
    public Map<Object, Long> getTopLockedKeys(int n) {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_LOCKED_KEYS, n);
        analyticsBean.resetStat(AnalyticsBean.Stat.MOST_LOCKED_KEYS);
        return res;
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys most contended")
    @Operation(displayName = "Top Contended Keys")
    public Map<Object, Long> getTopContendedKeys() {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_CONTENDED_KEYS);
        //analyticsBean.resetStat(AnalyticsBean.Stat.MOST_CONTENDED_KEYS);
        return res;
    }

    @ManagedOperation(description = "Show the top n keys most contended")
    @Operation(displayName = "Top Contended Keys")
    public Map<Object, Long> getTopContendedKeys(int n) {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_CONTENDED_KEYS, n);
        analyticsBean.resetStat(AnalyticsBean.Stat.MOST_CONTENDED_KEYS);
        return res;
    }

    @ManagedAttribute(description = "Show the top " + AnalyticsBean.MAX_CAPACITY + " keys whose lock acquisition failed by timeout")
    @Operation(displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
    public Map<Object, Long> getTopLockFailedKeys() {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_FAILED_KEYS);
        //analyticsBean.resetStat(AnalyticsBean.Stat.MOST_FAILED_KEYS);
        return res;
    }

    @ManagedOperation(description = "Show the top n keys whose lock acquisition failed ")
    @Operation(displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
    public Map<Object, Long> getTopLockFailedKeys(int n) {
        Map<Object, Long> res = analyticsBean.getTopKFrom(AnalyticsBean.Stat.MOST_FAILED_KEYS, n);
        analyticsBean.resetStat(AnalyticsBean.Stat.MOST_FAILED_KEYS);
        return res;
    }

}
