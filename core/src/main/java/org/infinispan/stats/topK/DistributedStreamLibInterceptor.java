package org.infinispan.stats.topK;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * Created by IntelliJ IDEA.
 * User: diego
 * Date: 05/01/12
 * Time: 18:55
 * To change this template use File | Settings | File Templates.
 */
public class DistributedStreamLibInterceptor extends StreamLibInterceptor {
private DistributionManager distributionManager;

    @Inject
    public void inject(DistributionManager distributionManager) {
        this.distributionManager = distributionManager;
    }

    @Override
    protected boolean isRemote(Object key) {
        return !distributionManager.getLocality(key).isLocal();
    }
}
