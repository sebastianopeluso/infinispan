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

/**
 * User: roberto
 * Date: 14/12/12
 * Time: 15:46
 */

public class TransactionTS {
   private long endLastTxTs;
   private long NTBC_execution_time;
   private long NTBC_count;

   public long getEndLastTxTs() {
      return endLastTxTs;
   }

   public void setEndLastTxTs(long endLastTxTs) {
      this.endLastTxTs = endLastTxTs;
   }

   public long getNTBC_execution_time() {
      return NTBC_execution_time;
   }

   public void addNTBC_execution_time(long NTBC_execution_time) {
      this.NTBC_execution_time += NTBC_execution_time;
   }

   public long getNTBC_count() {
      return NTBC_count;
   }

   public void addNTBC_count() {
      this.NTBC_count++;
   }
}
