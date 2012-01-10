package eu.cloudtm.rmi.statistics;

import org.omg.CORBA.PUBLIC_MEMBER;

/**
 * Created by IntelliJ IDEA.
 * User: diego
 * Date: 28/12/11
 * Time: 15:38
 * To change this template use File | Settings | File Templates.
 */

public class Statistics {

    public static final int NUM_STATS = 100;


    public static final int NUM_PREPARES = 2;
    public static final int LOCAL_EXEC = 3;
    public static final int WAITING_TIME_ON_LOCKS = 4;
    public static final int COMMIT_COMMAND_SIZE = 5;
    public static final int PREPARE_COMMAND_SIZE = 7;
    public static final int RTT = 8;
    public static final int NUM_RTTS = 9;
    public static final int LOCAL_EXEC_RO = 10;
    public static final int NUM_STARTED_WRITE_TX = 12;
    public static final int NUM_WAITED_FOR_LOCKS = 14;
    public static final int NUM_ROLLBACKS = 15;
    public static final int ROLLBACK_COMMAND_COST = 16;
    public static final int COMMIT_COMMAND_COST = 17;
    public static final int TOTAL_EXEC = 18;
    public static final int NUM_TAKEN_LOCKS = 19;
    public static final int NUM_COMMITTED_LOCAL_LOCKS = 22;
    public static final int HOLD_TIME = 23;
    public static final int LOCAL_HOLD_TIME = 24;
    public static final int REMOTE_HOLD_TIME = 25;
    public static final int AVG_REMOTE_EXEC = 26;
    public static final int MAX_REMOTE_EXEC = 27;
    public static final int NUM_LOCAL_HELD_LOCKS = 28;
    public static final int NUM_REMOTE_HELD_LOCKS = 29;
    public static final int LOCAL_SUX_HOLD_TIME = 30;
    public static final int REMOTE_SUX_HOLD_TIME = 31;
    public static final int LOCAL_SUX_HELD_LOCKS = 32;
    public static final int REMOTE_SUX_HELD_LOCKS = 33;
    public static final int NUM_STARTED_TX_RO = 34;
    public static final int NUM_STARTED_TX_WR = 35;
    public static final int NUM_COMMITTED_TX_RO = 36;
    public static final int NUM_COMMITTED_TX_WR = 37;
    public static final int NUM_ACQUIRED_LOCKS_WITHOUT_WAITING = 39;
    public static final int ACQUISITION_TIME_FOR_LOCKS = 40;
    public static final int NUM_LOCAL_LOCAL_CONFLICTS = 41;
    public static final int NUM_LOCAL_REMOTE_CONFLICTS = 42;
    public static final int LOCAL_EXEC_NO_CONT = 43;
    public static final int NUM_CLUSTERED_GET_COMMANDS = 44;
    public static final int CLUSTERED_GET_COMMAND_SIZE = 45;
    public static final int NUM_REMOTE_GETS = 46;
    public static final int REMOTE_GET_COST = 47;
    public static final int NUM_TIMEOUT_EXCEPTION_ON_PREPARE = 48;
    public static final int NUM_DEADLOCK_EXCEPTION_ON_PREPARE = 49;
    public static final int NUM_NODES_INVOLVED_IN_PREPARE = 50;
    public static final int NUM_PUTS_ON_LOCAL_KEY  = 51;
    public static final int NUM_PUTS_ON_REMOTE_KEY = 52;
    public static final int NUM_GETS_ON_LOCAL_KEY  = 53;
    public static final int NUM_GETS_ON_REMOTE_KEY = 54;

    /*
    public static final int NUM_OBJECTS = 10;
    public static final int REMOTE_GETS = 1;
    public static final int REMOTE_PUTS = 2;
    public static final int LOCAL_GETS = 3;
    public static final int LOCAL_PUTS = 4;
    public static final int MOST_LOCKED_KEYS = 5;
    public static final int MOST_CONTENDED_KEYS = 6;
    public static final int MOST_TIMEOUT_KEYS = 7;
    public static final int MOST_DEADLOCK_KEY = 8;
    */

}
