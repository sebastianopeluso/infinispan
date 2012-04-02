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

package org.infinispan.commands;


import org.infinispan.commands.read.AbstractLocalCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * User: Fabio Mariotti Date: 30/04/12
 */
public class SetClassCommand extends AbstractLocalCommand implements VisitableCommand {

   private String transactionalClass;

   public SetClassCommand(String transactionalClass) {
      this.transactionalClass = transactionalClass;
   }

   public SetClassCommand() {}

   public String getTransactionalClass() {
      return transactionalClass;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return null;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitSetClassCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }
}
