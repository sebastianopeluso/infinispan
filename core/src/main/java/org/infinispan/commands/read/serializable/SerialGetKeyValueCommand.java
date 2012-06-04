package org.infinispan.commands.read.serializable;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class SerialGetKeyValueCommand extends GetKeyValueCommand {

   private static final Log log = LogFactory.getLog(SerialGetKeyValueCommand.class);

   public SerialGetKeyValueCommand(Object key, CacheNotifier notifier,
                                   Set<Flag> flags) {
      super(key, notifier, flags);
   }

   public SerialGetKeyValueCommand() {
      super();
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry entry = ctx.getLastReadKey();

      if (entry == null || entry.isNull()) {
         if (log.isTraceEnabled()) {
            log.trace("Entry not found");
         }
         return null;
      }
      if (entry.isRemoved()) {
         if (log.isTraceEnabled()) {
            log.tracef("Entry has been deleted and is of type %s", entry.getClass().getSimpleName());
         }
         return null;
      }
      final Object value = entry.getValue();
      // FIXME: There's no point in notifying twice.
      notifier.notifyCacheEntryVisited(key, value, true, ctx);
      final Object result = returnCacheEntry ? entry : value;
      if (log.isTraceEnabled()) {
         log.tracef("Found value %s", result);
      }
      notifier.notifyCacheEntryVisited(key, value, false, ctx);
      return result;
   }
}
