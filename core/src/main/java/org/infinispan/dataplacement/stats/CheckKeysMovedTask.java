package org.infinispan.dataplacement.stats;

import org.infinispan.dataplacement.ObjectPlacementManager;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Task that checks the number of keys wrongly moved out
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class CheckKeysMovedTask implements Runnable {

   private final Set<Object> keysMoved;
   private final Set<Object> keysToMove;
   private final Stats stats;

   public CheckKeysMovedTask(Collection<Object> keysMoved, ObjectPlacementManager manager, Stats stats) {
      this.keysMoved = new HashSet<Object>(keysMoved);
      this.keysToMove = new HashSet<Object>(manager.getKeysToMove());
      this.stats = stats;
   }


   @Override
   public void run() {
      keysMoved.removeAll(keysToMove);
      stats.wrongKeyMovedErrors(keysMoved.size());
   }
}
