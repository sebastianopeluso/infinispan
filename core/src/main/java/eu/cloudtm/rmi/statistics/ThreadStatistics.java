package eu.cloudtm.rmi.statistics;


import java.io.Serializable;
import java.lang.Thread.State;
import java.util.HashMap;


/**
 * Created by IntelliJ IDEA.
 * User: Davide
 * Date: 20-apr-2011
 * Time: 16.48.47
 * To change this template use File | Settings | File Templates.
 */
public class ThreadStatistics implements Serializable, ISPNStats {


    private static final long serialVersionUID = 7450205779990714462L;

    private Thread thread = null;
    private State thread_state;

    private CurrentTransactionState currentTransactionState = null;


    private double arrayStats[] = new double[Statistics.NUM_STATS];
    //Can't create array of linkedList
    //private List<List<Object>> listKeys = new LinkedList<List<Object>>();



    private boolean needReset = false;


    /*
    //started transactions
    private Long lastTransactions = new Long(0);
    */

    public ThreadStatistics(Thread thread) {
        this.thread = thread;
        this.currentTransactionState = new CurrentTransactionState();
        //This has to be pre-populated so that I can reference the lists by index in the right order
        /*
        for(int i=0;i<Statistics.NUM_OBJECTS;i++){
            listKeys.add(i,new LinkedList<Object>());
        }
        */
    }

    public void flush(boolean commit, boolean isLocal) {
        this.currentTransactionState.flushTransaction(commit, isLocal);
        this.currentTransactionState = new CurrentTransactionState();
    }

    public void terminateLocalExecution() {
        this.currentTransactionState.terminateLocalExecution();
    }


    public Thread getThread() {
        return thread;
    }


    public State getThread_state() {
        return thread_state;
    }

    /*
    private void writeObject(ObjectOutputStream out) throws IOException {
        //out.defaultWriteObject();
        out.writeObject(thread.getState());


        //DIE
        out.writeObject(this.commitCommands);
        out.writeObject(this.commitCommandSize);
        out.writeObject(this.prepareCommands);
        out.writeObject(this.prepareCommandSize);

        System.out.println("Write InifnispanStatistics Object");
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        //in.defaultReadObject();
        System.out.println("Start Read InifnispanStatistics Object");
        this.thread_state = (State) in.readObject();

        //DIE
        this.commitCommands = (Long) in.readObject();
        this.commitCommandSize = (Long) in.readObject();
        this.prepareCommands = (Long) in.readObject();
        this.prepareCommandSize = (Long) in.readObject();

        System.out.println("Read InifnispanStatistics Object");
    }
    */

    //DIE

    public void startTransaction() {
        long time = System.nanoTime();
        //System.out.println(Thread.currentThread().getId() + " vado a fare la start a un tempo " + time);
        this.currentTransactionState.setInitTime(time);
    }

    public void addCommitCommandSize(long l) {
        this.currentTransactionState.setCommitCommandSize(l);
    }

    public void addRemoteGetCost(long l) {
        this.currentTransactionState.addRemoteGetCost(l);
    }

    public void addPrepareCommandSize(long l) {
        this.currentTransactionState.setPrepareCommandSize(l);
    }

    public void incrementToLocalConflicts() {
        this.currentTransactionState.incrementToLocalConflicts();
    }

    public void incrementToRemoteConflicts() {
        this.currentTransactionState.incrementToRemoteConflicts();
    }


    public void computeHoldTime(Object o, boolean isLocal, boolean commit) {
        this.currentTransactionState.computeHoldTime(o, commit, isLocal);
    }


    public void addLockWaitingTime(Long l) {
        this.currentTransactionState.addLockWaitingTime(l);

    }

    public void addClusteredGetCommandSize(long l) {
        this.currentTransactionState.addClusteredGetCommandSize(l);
    }


    public void incrementRequestedLocks() {
        this.currentTransactionState.incrementRequestedLocks();
    }

    public void startHoldTime(Object o, boolean isLocal) {
        this.currentTransactionState.startHoldTime(o);
    }

    //Updates the hold times in the case the transaction aborts
    public void flushHoldTimesOnAbort(boolean isLocal) {
        this.currentTransactionState.flushHoldTimesOnAbort(isLocal);
    }

    public void incrementTimeoutExceptionOnPrepare() {
        this.currentTransactionState.setTimeoutExceptionOnPrepare();
    }

    public void incrementDeadlockExceptionOnPrepare() {
        this.currentTransactionState.setDeadlockExceptionOnPrepare();
    }


    public void addRtt(Long l) {
        this.currentTransactionState.setRtt(l);
    }

    public void addMaxReplayTime(Long l) {
        this.currentTransactionState.setMaxReplayTime(l);
    }

    public void addAvgReplayTime(Long l) {
        this.currentTransactionState.setAvgReplayTime(l);
    }


    public void addCommitCost(Long l) {
        this.currentTransactionState.setCommitCommandCost(l);
    }

    public void addRollbackCost(Long l) {
        this.currentTransactionState.setRollbackCommandCost(l);
    }

    public void addLockAcquisitionTime(long l) {
        this.currentTransactionState.addAcquiringTime(l);
    }

    public void setNumNodesInvolvedInPrepare(long l) {
        this.currentTransactionState.setNodeInvolvedInPrepare(l);
    }

    public void incrementPut(boolean remote) {
        this.currentTransactionState.incrementPut(remote);
    }

    public void incrementGet(boolean remote) {
        this.currentTransactionState.incrementGet(remote);
    }


    private class CurrentTransactionState {
        private boolean stillLocalExecution;
        private long localExecution;

        private boolean isReadOnly;
        private long initTime;
        private long lastOperationTime;

        private long lockWaitingTime;
        private long lockNeitAcquiringTime; //if no contention arises
        private long waitedForLocks;
        private long holdTime;
        private long heldTimes;
        private long toLocalConflicts;
        private long toRemoteConflicts;
        private long acquiredLocksWithoutConflicts;
        private long averageReplayTime;
        private long maxReplayTime;

        private long commitCommandSize;
        private long prepareCommandSize;
        private long commitCommandCost;
        private long rollbackCommandCost;
        private long nodeInvolvedInPrepare;

        private long clusteredGetCommandSize;
        private long clusteredGetCommands;

        private long numRemoteGets;
        private long remoteGetCost;

        private int exceptionOnPrepare;

        private long rtt;

        private long totalDuration;

        private HashMap<Object, Long> holdTimes = new HashMap<Object, Long>();


        private long numPuts;

        //Pedro
        private long numLocalGet;
        private long numRemoteGet;
        private long numLocalPut;
        private long numRemotePut;



        /*
        //PEDRO'S
        private List<Object> remoteGet;
        private List<Object> localGet;
        private List<Object> remotePut;
        private List<Object> localPut;

        private List<Object> mostLockedKey;
        private List<Object> mostContendedKey; //keys with more contention detected
        //these could be just one object, and mutually exclusive
        //not changed to guarantee compatbility with Pedro's API
        private List<Object> mostFailedKeyByDeadlock; //keys that the lock acquisition fails because of deadlock
        private List<Object> mostFailedKeyByTimeout;  //keys that the lock acquisition fails because of timeout
        */

        public CurrentTransactionState() {
            this.initTime = 0L;
            this.isReadOnly = true; //as far as it does not tries to perform a put operation
            this.stillLocalExecution = true; //it is meaningful only if the tx is local
            this.holdTimes = new HashMap<Object, Long>();
            /*
            this.remoteGet = new LinkedList<Object>();
            this.localGet = new LinkedList<Object>();
            this.remotePut = new LinkedList<Object>();
            this.localPut = new LinkedList<Object>();
            this.mostContendedKey = new LinkedList<Object>();
            this.mostLockedKey = new LinkedList<Object>();
            this.mostFailedKeyByDeadlock = new LinkedList<Object>();
            this.mostFailedKeyByTimeout = new LinkedList<Object>();
            */

        }

        //Pedro
        public void incrementGet(boolean remote) {
            if(remote) {
                numRemoteGet++;
            } else {
                numLocalGet++;
            }
        }

        public void incrementPut(boolean remote) {
            if(remote) {
                numRemotePut++;
                this.isReadOnly = false;
            } else {
                numLocalPut++;
                this.isReadOnly = false;
            }
        }

        public void setNodeInvolvedInPrepare(long l) {
            this.nodeInvolvedInPrepare = l;
        }

        public void setCommitCommandSize(long l) {
            this.commitCommandSize = l;
        }

        public void setTimeoutExceptionOnPrepare() {
            this.exceptionOnPrepare = Statistics.NUM_TIMEOUT_EXCEPTION_ON_PREPARE;
        }

        public void setDeadlockExceptionOnPrepare() {
            this.exceptionOnPrepare = Statistics.NUM_DEADLOCK_EXCEPTION_ON_PREPARE;
        }


        public void setPrepareCommandSize(long l) {
            this.prepareCommandSize = l;
        }

        public void incrementToLocalConflicts() {
            this.toLocalConflicts++;
        }

        public void incrementToRemoteConflicts() {
            this.toRemoteConflicts++;
        }



        public void addLockWaitingTime(long l) {
            this.lockWaitingTime += l;
            this.waitedForLocks++;
        }

        public void addAcquiringTime(long l) {
            this.lockNeitAcquiringTime += l;
            this.acquiredLocksWithoutConflicts++;
        }

        public void setAvgReplayTime(long l) {
            this.averageReplayTime = l;
        }

        public void setMaxReplayTime(long l) {
            this.maxReplayTime = l;
        }

        public void setRollbackCommandCost(long l) {
            this.rollbackCommandCost = l;
        }

        public void setCommitCommandCost(long l) {
            this.commitCommandCost = l;
        }

        public void setRtt(long l) {
            this.rtt = l;
        }

        public void setInitTime(long l) {
            /*if (this.initTime != 0L)
                System.out.println(Thread.currentThread().getId() + " InitTime gia' settato a " + initTime);
            else {
            */    this.initTime = l;
            //}
        }


        public void flushTransaction(boolean commit, boolean isLocal) {
            this.lastOperationTime = System.nanoTime();
            this.totalDuration = this.lastOperationTime - this.initTime;


            if (isLocal) {

                addParameter(Statistics.CLUSTERED_GET_COMMAND_SIZE, this.clusteredGetCommandSize);
                addParameter(Statistics.NUM_CLUSTERED_GET_COMMANDS, this.clusteredGetCommands);


                addParameter(Statistics.NUM_GETS_ON_LOCAL_KEY, this.numLocalGet);
                addParameter(Statistics.NUM_GETS_ON_REMOTE_KEY, this.numRemoteGet);
                /*
                addObject(Statistics.LOCAL_GETS,this.localGet);
                addObject(Statistics.REMOTE_GETS,this.remoteGet);
                */

                //ATTENTION: THIS WORKS ONLY IF THE RO-TXs ACTUALLY VISIT THE COMMITCOMMAND!!!
                if (this.isReadOnly) {   //Local && RO
                    addParameter(Statistics.NUM_STARTED_TX_RO, 1);
                    if (commit) { //Local && RO && Committing
                        //Incrementing the number of started and committed ro transactions
                        addParameter(Statistics.NUM_COMMITTED_TX_RO, 1);

                        addParameter(Statistics.LOCAL_EXEC_RO, this.totalDuration);

                        StatisticsListManager.insertReadOnlyTXDurationRespTimeDistribution((int) ((double) this.totalDuration / 1000.0D));
                    } else {
                        //RO-TX ALWAYS COMMITS
                    }
                }


                //WRITE TRANSACTION FTW

                else {//Local && WR
                    addParameter(Statistics.NUM_STARTED_TX_WR, 1);

                    addParameter(Statistics.WAITING_TIME_ON_LOCKS, this.lockWaitingTime);
                    addParameter(Statistics.NUM_WAITED_FOR_LOCKS, this.waitedForLocks);
                    addParameter(Statistics.NUM_ACQUIRED_LOCKS_WITHOUT_WAITING, this.acquiredLocksWithoutConflicts);
                    addParameter(Statistics.ACQUISITION_TIME_FOR_LOCKS, this.lockNeitAcquiringTime);

                    addParameter(Statistics.NUM_LOCAL_LOCAL_CONFLICTS, this.toLocalConflicts);
                    addParameter(Statistics.NUM_LOCAL_REMOTE_CONFLICTS, this.toRemoteConflicts);

                    addParameter(Statistics.NUM_PUTS_ON_LOCAL_KEY, this.numLocalPut);
                    addParameter(Statistics.NUM_PUTS_ON_REMOTE_KEY, this.numRemotePut);

                    addParameter(this.exceptionOnPrepare, 1);
                    /*
                    addObject(Statistics.LOCAL_PUTS,this.localPut);
                    addObject(Statistics.REMOTE_PUTS,remotePut);
                    addObject(Statistics.MOST_CONTENDED_KEYS,this.mostContendedKey);
                    addObject(Statistics.MOST_LOCKED_KEYS,this.mostLockedKey);
                    addObject(Statistics.MOST_DEADLOCK_KEY,this.mostFailedKeyByDeadlock);
                    addObject(Statistics.MOST_TIMEOUT_KEYS,this.mostFailedKeyByTimeout);
                    */


                    if (commit) {//Local && WR && Committing

                        StatisticsListManager.insertWriteTXDurationRespTimeDistribution((int) ((double) this.totalDuration / 1000.0D));

                        addParameter(Statistics.NUM_COMMITTED_TX_WR, 1);
                        addParameter(Statistics.TOTAL_EXEC, this.totalDuration);
                        addParameter(Statistics.LOCAL_EXEC, localExecution);

                        addParameter(Statistics.NUM_RTTS, 1);
                        addParameter(Statistics.RTT, this.rtt);
                        addParameter(Statistics.AVG_REMOTE_EXEC, this.averageReplayTime);
                        addParameter(Statistics.MAX_REMOTE_EXEC, this.maxReplayTime);

                        addParameter(Statistics.COMMIT_COMMAND_COST, this.commitCommandCost);
                        addParameter(Statistics.COMMIT_COMMAND_SIZE, this.commitCommandSize);
                        addParameter(Statistics.PREPARE_COMMAND_SIZE, this.prepareCommandSize);
                        addParameter(Statistics.NUM_PREPARES, 1);
                        addParameter(Statistics.NUM_NODES_INVOLVED_IN_PREPARE, this.nodeInvolvedInPrepare);

                        addParameter(Statistics.LOCAL_EXEC_NO_CONT, (this.localExecution - this.lockWaitingTime - this.lockNeitAcquiringTime));


                    } else { //Local && WR && Aborting
                        addParameter(Statistics.NUM_ROLLBACKS, 1);
                        addParameter(Statistics.ROLLBACK_COMMAND_COST, this.rollbackCommandCost);

                        if (this.stillLocalExecution) {   //Dead before prepare
                            //We don't take statistics about txs half-way dead
                        } else { //Remotely Dead during validation phase
                            addParameter(Statistics.PREPARE_COMMAND_SIZE, this.prepareCommandSize);
                            addParameter(Statistics.NUM_PREPARES, 1);
                            addParameter(Statistics.NUM_NODES_INVOLVED_IN_PREPARE, this.nodeInvolvedInPrepare);
                            addParameter(Statistics.LOCAL_EXEC, localExecution); //anyway it has reached prepare-phase
                            addParameter(Statistics.LOCAL_EXEC_NO_CONT, (this.localExecution - this.lockWaitingTime - this.lockNeitAcquiringTime));
                        }
                    }

                }

            } else {
                //REMOTE TRANSACTION       --->update only lock stats
                addParameter(Statistics.REMOTE_GET_COST, this.remoteGetCost);
                addParameter(Statistics.NUM_REMOTE_GETS, this.numRemoteGets);
                //LOCKS STATS ARE UPDATED UPON UNLOCKING (BOTH FOR COMMIT AND ABORT)
            }



            if (needReset) {
                //Reset double array
                arrayStats = new double[Statistics.NUM_STATS];
                //Reset linkedList with accessed objects
                /*
                for(int i=0;i<Statistics.NUM_OBJECTS;i++){
                    listKeys.add(i,new LinkedList<Object>());
                }
                */
                needReset = false;
            }
        }

        public void terminateLocalExecution() {

            this.stillLocalExecution = false;
            this.localExecution = System.nanoTime() - this.initTime;
        }

        public void addClusteredGetCommandSize(long l) {
            this.clusteredGetCommandSize += l;
            this.clusteredGetCommands++;
        }


        //computes the hold time for a given  lock. If the lock is the last one being unlocked, the values of hold time are updated
        public void computeHoldTime(Object o, boolean commit, boolean isLocal) {
            //NB: this method is called also for Objects which have NOT been locked
            if (this.holdTimes.containsKey(o)) {
                this.holdTime += (System.nanoTime() - this.holdTimes.get(o));
                this.heldTimes++;
                this.holdTimes.remove(o);

                if (this.holdTimes.isEmpty()) {
                    if (isLocal) {
                        addParameter(Statistics.LOCAL_HOLD_TIME, holdTime);
                        addParameter(Statistics.NUM_LOCAL_HELD_LOCKS, heldTimes);

                        if (commit) {
                            addParameter(Statistics.LOCAL_SUX_HOLD_TIME, holdTime);
                            addParameter(Statistics.LOCAL_SUX_HELD_LOCKS, heldTimes);
                            addParameter(Statistics.NUM_COMMITTED_LOCAL_LOCKS, heldTimes);
                        }
                    } else {
                        addParameter(Statistics.REMOTE_HOLD_TIME, holdTime);
                        addParameter(Statistics.NUM_REMOTE_HELD_LOCKS, heldTimes);
                        if (commit) {
                            addParameter(Statistics.REMOTE_SUX_HOLD_TIME, holdTime);
                            addParameter(Statistics.REMOTE_SUX_HELD_LOCKS, heldTimes);
                        }
                    }
                    //aggregate stats
                    addParameter(Statistics.NUM_TAKEN_LOCKS, heldTimes);
                    addParameter(Statistics.HOLD_TIME, holdTime);


                }
            }
        }

        public void flushHoldTimesOnAbort(boolean isLocal) {

            for (Object o : this.holdTimes.keySet()) {
                this.holdTime += System.nanoTime() - this.holdTimes.get(o);
                this.heldTimes++;
            }
            if (isLocal) {
                addParameter(Statistics.LOCAL_HOLD_TIME, holdTime);
                addParameter(Statistics.NUM_LOCAL_HELD_LOCKS, heldTimes);
            } else {
                addParameter(Statistics.REMOTE_HOLD_TIME, holdTime);
                addParameter(Statistics.NUM_REMOTE_HELD_LOCKS, heldTimes);
            }
            addParameter(Statistics.NUM_TAKEN_LOCKS, heldTimes);
            addParameter(Statistics.HOLD_TIME, holdTime);
        }

        public void startHoldTime(Object o) {
            //this control should be useless since a lock is requested only if it's not already locked, but still...
            if (!this.holdTimes.containsKey(o)) {
                this.holdTimes.put(o, System.nanoTime());
            }
        }

        public void incrementRequestedLocks() {

        }

        public void addRemoteGetCost(long l) {
            this.remoteGetCost += l;
            this.numRemoteGets++;
        }

        /*
        public void addLockInformation(Object key, boolean contention, boolean deadlock, boolean timeout) {

            this.mostLockedKey.add(key);

            if (contention) {
                this.mostContendedKey.add(key);
            }

            if (deadlock) {
                this.mostFailedKeyByDeadlock.add(key);
            } else if (timeout) {
                this.mostFailedKeyByTimeout.add(key);
            }
        }


        public void addRemoteGet(Object o){
            this.remoteGet.add(o);
        }
        public void addRemotePut(Object o){
            this.remotePut.add(o);
        }
        public void addLocalGet(Object o){
           this.localGet.add(o);
        }
        public void addLocalPut(Object o){
           this.localPut.add(o);
        }
        */

    }


    public double getParameter(int index) {
        return this.arrayStats[index];
    }

    public void addParameter(int index, double delta) {
        this.arrayStats[index] += delta;
    }

    public void setParameter(int index, double value) {
        this.arrayStats[index] = value;
    }
    /*
    //Ad a list of object to the list indexed at place index
    public void addObject(int index, List<Object>list){
        this.listKeys.get(index).add(list);
    }

    public List<Object> getObjects(int index){
        return this.listKeys.get(index);
    }
    */

    //not thread-safe
    public void reset() {
        this.needReset = true;
    }


}

