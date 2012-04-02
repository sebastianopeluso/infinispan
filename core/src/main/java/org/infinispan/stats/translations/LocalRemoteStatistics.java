package org.infinispan.stats.translations;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 03/05/12
 */
public class LocalRemoteStatistics {

   private final static int COMMON_STATS = 24;

   protected final static int numLocalRemoteStatistcs = COMMON_STATS;

   public static final int LOCK_HOLD_TIME = 0;
   public static final int NUM_HELD_LOCK = 1;
   public static final int LOCK_WAITING_TIME = 2;
   public static final int ROLLBACK_EXECUTION_TIME = 3;
   public static final int COMMIT_EXECUTION_TIME = 4;
   public static final int NUM_COMMIT_COMMAND = 5;
   public static final int NUM_ROLLBACKS = 6;
   public static final int NUM_PUTS = 7;
   public static final int NUM_SUCCESSFUL_RTTS = 8;
   public static final int NUM_WAITED_FOR_LOCKS = 9;
   public static final int NUM_COMMITTED_RO_TX = 10;
   public static final int NUM_ABORTED_RO_TX = 11;
   public static final int NUM_ABORTED_WR_TX = 12;
   public static final int NUM_COMMITTED_WR_TX = 13;
   public static final int LOCK_CONTENTION_TO_LOCAL = 14;
   public static final int LOCK_CONTENTION_TO_REMOTE = 15;
   public static final int NUM_REMOTE_GET = 16;
   public static final int NUM_REMOTE_PUT = 17;
   public static final int REMOTE_GET_EXECUTION = 18;
   public static final int REMOTE_PUT_EXECUTION = 19;
   public static final int WR_TX_ABORTED_EXECUTION_TIME = 20;
   public static final int RO_TX_ABORTED_EXECUTION_TIME = 21;
   public static final int WR_TX_SUCCESSFUL_EXECUTION_TIME = 22;
   public static final int RO_TX_SUCCESSFUL_EXECUTION_TIME = 23;
}
