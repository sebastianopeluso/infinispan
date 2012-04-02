package org.infinispan.stats.translations;

/**
 * Created by IntelliJ IDEA.
 * User: diego
 * Date: 28/12/11
 * Time: 15:38
 * To change this template use File | Settings | File Templates.
 */

public class ExposedStatistics {


   public static final int NUM_STATS = IspnStats.values().length;
   public static final int NUM_TX_CLASSES = TransactionalClasses.values().length;


/*
The idea is having IspnStats which are used outside the stats module to refer to a statistic (update or retrieval)
Then local and remote transactions implement a translation to point to their own statistic relevant to the input one
Of course we can bypass this if we use a structure which is NOT accessed by index (i.e., an array), but another one
i.e. HashMap. In the second case I could share the statistics naming between upper and lower layer. I just don't know
if accessing an HashMap with a Object value and doing a get-modify-put is as cheap as accessing directly to the entry of an array,
even if this implies the penalty of a big switch. Please not that the usefulness of the switch is that it can detect
if a module is dealing with a non-encompassed statistic (and gives much more modularity if we want to exend the hierarchy of
TransactionStatistics since every class implements its own case switch


*/

   public enum TransactionalClasses {
      DEFAULT_CLASS
   }


   //TODO change: every statistic that is collected must be available to be queried
   //TODO it is not true the inverse: so I have to define a class LowLevelStats and then a class
   //TODO like HighLevelStats which extends LowLevelStats.
   //TODO Both the classes implements QuerableStats
   //TODO The classes just contain integers which replace the current "mixed" enum


   public enum IspnStats {
      LOCK_WAITING_TIME,  // C
      LOCK_HOLD_TIME,     // C
      NUM_HELD_LOCKS,     // C
      ROLLBACK_EXECUTION_TIME, // C
      NUM_ROLLBACKS, // C
      WR_TX_LOCAL_EXECUTION_TIME,   // L
      RTT, // L
      REPLAY_TIME, // R
      REPLAYED_TXS, // R
      PREPARE_COMMAND_SIZE, // L
      NUM_COMMITTED_RO_TX,  // C
      NUM_COMMITTED_WR_TX,  // C
      NUM_ABORTED_WR_TX,   // C
      NUM_ABORTED_RO_TX,   // C
      NUM_SUCCESSFUL_RTTS,  // L
      NUM_PREPARES,  // L
      NUM_PUTS,  // C
      COMMIT_EXECUTION_TIME, // L
      LOCAL_EXEC_NO_CONT,  // ONLY FOR QUERY, derived on the fly
      LOCAL_CONTENTION_PROBABILITY, // ONLY FOR QUERY, derived on the fly
      LOCK_CONTENTION_TO_LOCAL, // C
      LOCK_CONTENTION_TO_REMOTE, // C
      NUM_SUCCESSFUL_PUTS, // C, this includes also repeated puts over the same item
      PUTS_PER_LOCAL_TX,  // ONLY FOR QUERY, derived on the fly
      NUM_WAITED_FOR_LOCKS, // C
      NUM_NODES_IN_PREPARE, //L
      NUM_REMOTE_GET, // C
      REMOTE_GET_EXECUTION, // C
      REMOTE_PUT_EXECUTION, // C
      NUM_REMOTE_PUT,  // C
      ARRIVAL_RATE, // ONLY FOR QUERY, derived on the fly
      TX_WRITE_PERCENTAGE, // ONLY FOR QUERY, derived on the fly
      SUCCESSFUL_WRITE_PERCENTAGE, // ONLY FOR QUERY, derived on the fly
      WR_TX_ABORTED_EXECUTION_TIME, // C
      WR_TX_SUCCESSFUL_EXECUTION_TIME, //C
      RO_TX_SUCCESSFUL_EXECUTION_TIME, //C
      RO_TX_ABORTED_EXECUTION_TIME,  //C
      NUM_COMMIT_COMMAND, //C
      APPLICATION_CONTENTION_FACTOR // ONLY FOR QUERY
   }




}
