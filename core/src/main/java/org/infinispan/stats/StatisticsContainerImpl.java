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
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class StatisticsContainerImpl implements StatisticsContainer{

   private final long[] stats;

   public StatisticsContainerImpl(int size){
      this.stats = new long[size];
   }

   public final void addValue(int param, double value){
      this.stats[param]+=value;
   }

   public final long getValue(int param){
      return this.stats[param];
   }

   public final void mergeTo(StatisticsContainer sc){
      int length = this.stats.length;
      for(int i = 0; i < length; i++){
         sc.addValue(i,this.stats[i]);
      }
   }

   public final int size(){
      return this.stats.length;
   }

   public final void dump(){
      for(int i=0; i<this.stats.length;i++){
         System.out.println("** "+i+" : "+stats[i]+" **");
      }
   }
}
