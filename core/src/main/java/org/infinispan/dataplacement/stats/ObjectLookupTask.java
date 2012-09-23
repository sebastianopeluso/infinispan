package org.infinispan.dataplacement.stats;

import org.infinispan.dataplacement.OwnersInfo;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Task that checks the number of keys that was move to a wrong node, the average query duration and the size of the
 * object lookup
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectLookupTask implements Runnable {

   private static final Log log = LogFactory.getLog(ObjectLookupTask.class);

   private final ObjectLookup objectLookup;
   private final Map<Object, OwnersInfo> ownersInfoMap;
   private final Stats stats;

   public ObjectLookupTask(Map<Object, OwnersInfo> ownersInfoMap, ObjectLookup objectLookup, Stats stats) {
      this.ownersInfoMap = ownersInfoMap;
      this.objectLookup = objectLookup;
      this.stats = stats;
   }

   @Override
   public void run() {
      int errors = 0;
      long start, end, duration = 0;
      for (Map.Entry<Object, OwnersInfo> entry : ownersInfoMap.entrySet()) {
         Set<Integer> expectedOwners = new TreeSet<Integer>(entry.getValue().getNewOwnersIndexes());
         start = System.currentTimeMillis();
         Collection<Integer> owners = objectLookup.query(entry.getKey());
         end = System.currentTimeMillis();
         Set<Integer> ownersQuery = new TreeSet<Integer>(owners);

         errors += expectedOwners.containsAll(ownersQuery) ? 0 : 1;
         duration += (end - start);
      }
      stats.wrongOwnersErrors(errors);
      stats.queryDuration(duration / ownersInfoMap.size());

      try {
         ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
         objectOutputStream.writeObject(objectLookup);
         objectOutputStream.flush();

         stats.objectLookupSize(byteArrayOutputStream.toByteArray().length);
      } catch (IOException e) {
         log.warn("Error calculating object lookup size", e);
      }
   }
}
