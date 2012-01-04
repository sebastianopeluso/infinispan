package org.infinispan.interceptors;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * Created by IntelliJ IDEA.
 * User: diego
 * Date: 02/01/12
 * Time: 21:13
 * To change this template use File | Settings | File Templates.
 */
public class DistCacheMgmtInterceptor extends CacheMgmtInterceptor {
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
