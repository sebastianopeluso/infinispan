package org.infinispan.transaction.gmu;


import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUEntryVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class CommitLog {

   private static final Log log = LogFactory.getLog(CommitLog.class);

   private VersionEntry currentVersion;
   private GMUVersionGenerator versionGenerator;

   @Inject
   public void inject(VersionGenerator versionGenerator){
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   //AFTER THE VersionVCFactory
   @Start(priority = 31)
   public void start() {
      currentVersion = new VersionEntry(toGMUEntryVersion(versionGenerator.generateNew()));
   }

   @Stop
   public void stop() {

   }

   public synchronized EntryVersion getCurrentVersion() {
      //versions are immutable
      EntryVersion version = currentVersion.getVersion();
      if (log.isTraceEnabled()) {
         log.tracef("getCurrentVersion() ==> %s", version);
      }
      return version;
   }


   public EntryVersion getAvailableVersionLessThan(EntryVersion other) {
      VersionEntry iterator;
      synchronized (this) {
         //if other is null, return the most recent version
         if (other == null || isLessOrEquals(currentVersion.getVersion(), other)) {
            EntryVersion version = currentVersion.getVersion();
            if (log.isTraceEnabled()) {
               log.tracef("getAvailableVersionLessThan(%s) ==> %s", other, version);
            }
            return version;
         }
         iterator = currentVersion.getPrevious();
      }

      while (iterator != null) {
         if (isLessOrEquals(iterator.getVersion(), other)) {
            EntryVersion version = iterator.getVersion();
            if (log.isTraceEnabled()) {
               log.tracef("getAvailableVersionLessThan(%s) ==> %s", other, version);
            }
            return version;
         }
         iterator = iterator.getPrevious();
      }
      throw new IllegalStateException("Version required no longer exists");
   }

   public synchronized void addNewVersion(EntryVersion newVersion) {
      VersionEntry current = new VersionEntry(toGMUEntryVersion(
            versionGenerator.mergeAndMax(currentVersion.getVersion(), newVersion)));
      if (current.getVersion().compareTo(currentVersion.getVersion()) == EQUAL) {
         //same version??
         if (log.isTraceEnabled()) {
            log.tracef("addNewVersion(%s) ==> %s", newVersion, currentVersion.getVersion());
         }
         return;
      }
      current.setPrevious(currentVersion);
      currentVersion = current;
      if (log.isTraceEnabled()) {
         log.tracef("addNewVersion(%s) ==> %s", newVersion, currentVersion.getVersion());
      }
      notifyAll();
   }

   public synchronized boolean waitForVersion(EntryVersion version, long timeout) throws InterruptedException {
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

   private boolean isLessOrEquals(EntryVersion version1, EntryVersion version2) {
      InequalVersionComparisonResult comparisonResult = version1.compareTo(version2);
      return comparisonResult == BEFORE_OR_EQUAL || comparisonResult == BEFORE || comparisonResult == EQUAL;
   }

   private static class VersionEntry {
      private final GMUEntryVersion version;
      private VersionEntry previous;

      private VersionEntry(GMUEntryVersion version) {
         this.version = version;
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
   }
}
