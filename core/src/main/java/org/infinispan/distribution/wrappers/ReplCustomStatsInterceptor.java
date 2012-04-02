package org.infinispan.distribution.wrappers;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 02/05/12
 */
public class ReplCustomStatsInterceptor extends CustomStatsInterceptor {

   @Override
    public boolean isRemote(Object key) {
        return false;
    }
}
