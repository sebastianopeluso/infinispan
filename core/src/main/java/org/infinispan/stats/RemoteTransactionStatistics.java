/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.stats.translations.ExposedStatistics;
import org.infinispan.stats.translations.RemoteStatistics;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class RemoteTransactionStatistics extends TransactionStatistics{

   public RemoteTransactionStatistics(Configuration configuration){
      super(RemoteStatistics.getSize(),configuration);
   }

   protected final void onPrepareCommand(){
      //nop
   }

   @Override
   protected final void terminate() {
      //nop
   }

   protected final int getIndex(ExposedStatistics.IspnStats stat) throws NoIspnStatException{
      int ret = RemoteStatistics.getIndex(stat);
      if (ret == RemoteStatistics.NOT_FOUND) {
         throw new NoIspnStatException("Statistic "+stat+" is not available!");
      }
      return ret;
   }

   @Override
   public final String toString() {
      return "RemoteTransactionStatistics{" + super.toString();
   }
}
