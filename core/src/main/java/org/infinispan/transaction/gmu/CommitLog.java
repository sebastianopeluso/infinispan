package org.infinispan.transaction.gmu;


import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;
import static org.infinispan.container.versioning.gmu.GMUEntryVersion.NON_EXISTING;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUEntryVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class CommitLog {

   private static final Log log = LogFactory.getLog(CommitLog.class);

   //private GMUEntryVersion mostRecentVersion;
   private VersionEntry currentVersion;
   private GMUVersionGenerator versionGenerator;
   private boolean enabled = false;

   @Inject
   public void inject(VersionGenerator versionGenerator, Configuration configuration){
      if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
         this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      }
      enabled = this.versionGenerator != null;
   }

   //AFTER THE VersionVCFactory
   @Start(priority = 31)
   public void start() {
      if (!enabled) {
         return;
      }
      currentVersion = new VersionEntry(toGMUEntryVersion(versionGenerator.generateNew()), Collections.emptySet());
      //mostRecentVersion = toGMUEntryVersion(versionGenerator.generateNew());
   }

   @Stop
   public void stop() {

   }

   public synchronized GMUEntryVersion getCurrentVersion() {
      assertEnabled();
      //versions are immutable
      //GMUEntryVersion version = versionGenerator.updatedVersion(mostRecentVersion);
      GMUEntryVersion version = versionGenerator.updatedVersion(currentVersion.getVersion());
      if (log.isTraceEnabled()) {
         log.tracef("getCurrentVersion() ==> %s", version);
      }
      return version;
   }


   public GMUEntryVersion getAvailableVersionLessThan(EntryVersion other) {
      assertEnabled();
      if (other == null) {
         return getCurrentVersion();
      }
      GMUEntryVersion otherVersion = toGMUEntryVersion(other);

      if (otherVersion.getThisNodeVersionValue() != NON_EXISTING) {
         return otherVersion;
      }

      LinkedList<GMUEntryVersion> possibleVersion = new LinkedList<GMUEntryVersion>();
      VersionEntry iterator;
      synchronized (this) {
         /*if (isLessOrEquals(mostRecentVersion, otherVersion)) {
            possibleVersion.add(mostRecentVersion);
         }*/
         iterator = currentVersion;
      }
      while (iterator != null) {
         if (isLessOrEquals(iterator.getVersion(), otherVersion)) {
            possibleVersion.add(iterator.getVersion());
         } else {
            possibleVersion.clear();
         }
         iterator = iterator.getPrevious();
      }
      return versionGenerator.mergeAndMax(possibleVersion.toArray(new GMUEntryVersion[possibleVersion.size()]));
   }

   public synchronized void insertNewCommittedVersions(Collection<CacheTransaction> transactions) {
      assertEnabled();
      for (CacheTransaction transaction : transactions) {
         VersionEntry current = new VersionEntry(toGMUEntryVersion(transaction.getTransactionVersion()),
                                                 Util.getAffectedKeys(transaction.getModifications(), null));
         current.setPrevious(currentVersion);
         currentVersion = current;
         //mostRecentVersion = versionGenerator.mergeAndMax(mostRecentVersion, currentVersion.getVersion());
      }
      if (log.isTraceEnabled()) {
         log.tracef("insertNewCommittedVersions(...) ==> %s", currentVersion.getVersion());
      }
      notifyAll();
   }

   public synchronized void updateMostRecentVersion(EntryVersion newVersion) {
      /*
      assertEnabled();
      GMUEntryVersion gmuEntryVersion = toGMUEntryVersion(newVersion);
      if (gmuEntryVersion.getThisNodeVersionValue() > mostRecentVersion.getThisNodeVersionValue()) {
         log.warn("Cannot update the most recent version to a version higher than " +
                                                  "the current version");
         return;
      }
      mostRecentVersion = versionGenerator.mergeAndMax(mostRecentVersion, gmuEntryVersion);
      */
   }

   public synchronized boolean waitForVersion(EntryVersion version, long timeout) throws InterruptedException {
      assertEnabled();
      if (timeout < 0) {
         if (log.isTraceEnabled()) {
            log.tracef("waitForVersion(%s,%s) and current version is %s", version, timeout, currentVersion.getVersion());
         }
         long versionValue = toGMUEntryVersion(version).getThisNodeVersionValue();
         while (currentVersion.getVersion().getThisNodeVersionValue() < versionValue) {
            wait();
         }
         if (log.isTraceEnabled()) {
            log.tracef("waitForVersion(%s) ==> %s TRUE ?", version,
                       currentVersion.getVersion().getThisNodeVersionValue());
         }
         return true;
      }
      long finalTimeout = System.currentTimeMillis() + timeout;
      long versionValue = toGMUEntryVersion(version).getThisNodeVersionValue();
      if (log.isTraceEnabled()) {
         log.tracef("waitForVersion(%s,%s) and current version is %s", version, timeout, currentVersion.getVersion());
      }
      do {
         if (currentVersion.getVersion().getThisNodeVersionValue() >= versionValue) {
            if (log.isTraceEnabled()) {
               log.tracef("waitForVersion(%s) ==> %s >= %s ?", version,
                          currentVersion.getVersion().getThisNodeVersionValue(), versionValue);
            }
            return true;
         }
         long waitingTime = finalTimeout - System.currentTimeMillis();
         if (waitingTime <= 0) {
            break;
         }
         wait(waitingTime);
      } while (true);
      if (log.isTraceEnabled()) {
         log.tracef("waitForVersion(%s) ==> %s >= %s ?", version,
                    currentVersion.getVersion().getThisNodeVersionValue(), versionValue);
      }
      return currentVersion.getVersion().getThisNodeVersionValue() >= versionValue;
   }

   public final boolean dumpTo(String filePath) {
      assertEnabled();
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         VersionEntry iterator;
         synchronized (this) {
            //bufferedWriter.write(mostRecentVersion.toString());
            iterator = currentVersion;
         }
         bufferedWriter.newLine();
         while (iterator != null) {
            iterator.dumpTo(bufferedWriter);
            iterator = iterator.getPrevious();
         }
         return true;
      } catch (IOException e) {
         return false;
      } finally {
         Util.close(bufferedWriter);
      }
   }

   private void assertEnabled() {
      if (!enabled) {
         throw new IllegalStateException("Commit Log not enabled!");
      }
   }

   private boolean isLessOrEquals(EntryVersion version1, EntryVersion version2) {
      InequalVersionComparisonResult comparisonResult = version1.compareTo(version2);
      return comparisonResult == BEFORE_OR_EQUAL || comparisonResult == BEFORE || comparisonResult == EQUAL;
   }

   private static class VersionEntry {
      private final GMUEntryVersion version;
      private final Object[] keysModified;
      private VersionEntry previous;

      private VersionEntry(GMUEntryVersion version, Set<Object> keysModified) {
         this.version = version;
         if (keysModified == null) {
            this.keysModified = null;
         } else {
            this.keysModified = keysModified.toArray(new Object[keysModified.size()]);
         }
      }

      public GMUEntryVersion getVersion() {
         return version;
      }

      public VersionEntry getPrevious() {
         return previous;
      }

      public void setPrevious(VersionEntry previous) {
         this.previous = previous;
      }

      @Override
      public String toString() {
         return "VersionEntry{" +
               "version=" + version +
               ", keysModified=" + (keysModified == null ? "ALL" : Arrays.asList(keysModified)) +
               '}';
      }

      public final void dumpTo(BufferedWriter writer) throws IOException {
         writer.write(version.toString());
         writer.write("=");
         writer.write((keysModified == null ? "ALL" : Arrays.asList(keysModified).toString()));
         writer.newLine();
         writer.flush();
      }
   }
}
