package org.infinispan.stats.translations;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 */
public class LocalStatistics extends LocalRemoteStatistics {
   private static final int NUM_ONLY_LOCAL_STATS = 9;
   private static final int offset = LocalRemoteStatistics.numLocalRemoteStatistcs;
   public static final int NUM_STATS = LocalRemoteStatistics.numLocalRemoteStatistcs + NUM_ONLY_LOCAL_STATS;


   public static final int LOCAL_CONTENTION_PROBABILITY = offset;
   public static final int WR_TX_LOCAL_EXECUTION_TIME = offset + 1;
   public static final int WR_TX_SUCCESSFUL_LOCAL_EXECUTION_TIME = offset + 2;
   public static final int PUTS_PER_LOCAL_TX = offset + 3;
   public static final int PREPARE_COMMAND_SIZE = offset + 4;
   public static final int NUM_NODES_IN_PREPARE = offset + 5;
   public static final int NUM_PREPARE = offset + 6;
   public static final int NUM_RTTS = offset + 7;
   public static final int RTT = offset + 8;
}
