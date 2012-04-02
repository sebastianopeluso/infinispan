package org.infinispan.distribution.wrappers;

import org.infinispan.atomic.Delta;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
public class EntryFactoryWrapper implements EntryFactory {

   private final EntryFactory actual;

   public EntryFactoryWrapper(EntryFactory actual) {
      this.actual = actual;
   }

   @Override
   public CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      System.out.println("EntryFactoryWrapper.wrapEntryForReading");
      return actual.wrapEntryForReading(ctx, key);
   }

   @Override
   public MVCCEntry wrapEntryForClear(InvocationContext ctx, Object key) throws InterruptedException {
      System.out.println("EntryFactoryWrapper.wrapEntryForClear");
      return actual.wrapEntryForClear(ctx, key);
   }

   @Override
   public MVCCEntry wrapEntryForReplace(InvocationContext ctx, Object key) throws InterruptedException {
      System.out.println("EntryFactoryWrapper.wrapEntryForReplace");
      return actual.wrapEntryForReplace(ctx, key);
   }

   @Override
   public MVCCEntry wrapEntryForRemove(InvocationContext ctx, Object key) throws InterruptedException {
      System.out.println("EntryFactoryWrapper.wrapEntryForRemove");
      return actual.wrapEntryForRemove(ctx, key);
   }

   @Override
   public CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta) throws InterruptedException {
      System.out.println("EntryFactoryWrapper.wrapEntryForDelta");
      return actual.wrapEntryForDelta(ctx, deltaKey, delta);
   }

   @Override
   public MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry ice, boolean undeleteIfNeeded, FlagAffectedCommand cmd) throws InterruptedException {
      return actual.wrapEntryForPut(ctx, key, ice, undeleteIfNeeded, cmd);
   }   
}
