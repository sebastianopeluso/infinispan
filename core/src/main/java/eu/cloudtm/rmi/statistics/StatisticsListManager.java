package eu.cloudtm.rmi.statistics;




import java.lang.Thread.State;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: Davide
 * Date: 20-apr-2011
 * Time: 16.08.54
 * To change this template use File | Settings | File Templates.
 */

public final class StatisticsListManager {

    //This is needed because I can traverse the list while still modifying it
    private static CopyOnWriteArrayList<ISPNStats> statsList=null;

    private StatisticsListManager() {}

    private static final boolean collapseBeforeQuery = false;

    private static long windowInitTime;

    private static InterArrivalHistogram interArrivalHistogram = new InterArrivalHistogram(0,10000000L,100);

    private static ReservoirSampling responseTimeWriteTXDistribution = new ReservoirSampling();
    private static ReservoirSampling responseTimeReadOnlyTXDistribution = new ReservoirSampling();

    public synchronized static CopyOnWriteArrayList<ISPNStats> getInfinispanStatisticsList(){

        if(statsList==null){
            statsList=new CopyOnWriteArrayList<ISPNStats>();
            windowInitTime = System.nanoTime();
        }
        return statsList;


    }

    public synchronized static void addInfinispanStatistics(ThreadStatistics thread) {

        if(statsList==null){
            statsList=new CopyOnWriteArrayList<ISPNStats>();
            windowInitTime = System.nanoTime();
        }
        statsList.add(thread);
    }

    public synchronized static void removeInfinispanStatistics(ISPNStats thread) {
        if(statsList==null)
            throw new RuntimeException("Trying to remove an entry from an empty statsList");
        statsList.remove(thread);
    }

    public static CopyOnWriteArrayList<ISPNStats> getList(){
        return statsList;
    }

    //We can safely remove elements from list since it is a copyOnWriteArrayList
    private synchronized static void clearList(){
        if(statsList==null)
            throw new RuntimeException("Trying to remove an entry from an empty statsList");
        Iterator<ISPNStats> iter = statsList.iterator();
        ISPNStats temp;
        while(iter.hasNext()){
            temp = iter.next();
            if(temp instanceof ThreadStatistics){
                ThreadStatistics ts = (ThreadStatistics) temp;
                if(ts.getThread_state().equals(State.TERMINATED))
                    statsList.remove(temp);
                else
                    temp.reset();
            }
            else if(temp instanceof NodeStats)
                statsList.remove(temp);

        }
    }

    public static void reset(){
        clearList();
        windowInitTime = System.nanoTime();
        responseTimeReadOnlyTXDistribution = new ReservoirSampling();
        responseTimeWriteTXDistribution = new ReservoirSampling();
    }


    public static void insertWriteTXDurationRespTimeDistribution(int time){
        responseTimeWriteTXDistribution.insertSample(time);
    }

    public static long getWriteTXDuration99Percentile(){
        return responseTimeWriteTXDistribution.get99Percentile();
    }

    public static long getWriteTXDuration95Percentile(){
        return responseTimeWriteTXDistribution.get95Percentile();
    }

    public static long getWriteTXDuration90Percentile(){
        return responseTimeWriteTXDistribution.get90Percentile();
    }

    public static long getWriteTXDurationKPercentile(int k){
        return responseTimeWriteTXDistribution.getKPercentile(k);
    }
    public static void insertReadOnlyTXDurationRespTimeDistribution(int time){
        responseTimeWriteTXDistribution.insertSample(time);
    }

    public static long getReadOnlyTXDuration99Percentile(){
        return responseTimeReadOnlyTXDistribution.get99Percentile();
    }

    public static long getReadOnlyTXDuration95Percentile(){
        return responseTimeReadOnlyTXDistribution.get95Percentile();
    }

    public static long getReadOnlyTXDuration90Percentile(){
        return responseTimeReadOnlyTXDistribution.get90Percentile();
    }

    public static long getReadOnlyTXDurationKPercentile(int k){
        return responseTimeReadOnlyTXDistribution.getKPercentile(k);
    }

    public static double getLocalConflictProbability(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getLocalConflictProbability();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.NUM_PUTS_ON_LOCAL_KEY,Statistics.NUM_PUTS_ON_REMOTE_KEY, Statistics.NUM_LOCAL_LOCAL_CONFLICTS, Statistics.NUM_LOCAL_REMOTE_CONFLICTS};
            double[] values = getParameters(param);
            double numPuts = values[0]+values[1];
            double numCont = values[2] + values[3];
            if(numPuts!=0)
                return numCont / numPuts;
            return 0D;
        }
    }

    public static long getLocalExecNoCont(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getLocalExecNoCont();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.LOCAL_EXEC_NO_CONT, Statistics.NUM_PREPARES};
            double[] values = getParameters(param);
            double localExec = values[0];
            double numPrepare = values[1];
            if(numPrepare!=0)
                return (long) (localExec / numPrepare);
            return 0L;
        }
    }

    public static long getCommitCommandSize(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getCommitCommandSize();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.COMMIT_COMMAND_SIZE, Statistics.NUM_COMMITTED_TX_WR};  //Only for write tx
            double[] values = getParameters(param);
            double commitSize = values[0];
            double numCommits = values[1];
            if(numCommits!=0){
                return (long)(commitSize / numCommits);
            }
            return 0L;
        }


    }

    public static long getPrepareCommandSize(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getPrepareCommandSize();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.PREPARE_COMMAND_SIZE, Statistics.NUM_PREPARES};
            double[] values = getParameters(param);
            double prepareSize = values[0];
            double numPrepares = values[1];
            if(numPrepares!=0){
                return (long)(prepareSize / numPrepares);
            }
            return 0L;
        }
    }

    public static long getRtt(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getRtt();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.RTT, Statistics.NUM_RTTS, Statistics.MAX_REMOTE_EXEC};
            double[] values = getParameters(param);
            double rtt = values[0];
            double rtts = values[1];
            double replay = values[2];
            if(rtts != 0){
                return (long)((rtt-replay) / rtts);
            }
            return 0L;
        }
    }

    public static long getLocalExec(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getLocalExec();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.LOCAL_EXEC, Statistics.NUM_PREPARES};
            double[] values = getParameters(param);
            double localExec = values[0];
            double numPrepares = values[1];
            if(numPrepares!=0){
                return (long)(localExec/numPrepares);
            }
            return 0L;
        }

    }

    public static long getLocalExecRO(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getLocalExecRO();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.LOCAL_EXEC_RO, Statistics.NUM_COMMITTED_TX_RO};
            double[] values = getParameters(param);
            double localExecRO = values[0];
            double numRO = values[1];
            if(numRO!=0){
                return (long)(localExecRO / numRO);
            }
            return 0L;
        }
    }

    public static long getTotalExec(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getTotalExec();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.TOTAL_EXEC, Statistics.NUM_COMMITTED_TX_WR};
            double[] values = getParameters(param);
            double totalExec = values[0];
            double numCommits = values[1];
            if(numCommits!=0){
                return (long)(totalExec / numCommits);
            }
            return 0L;
        }
    }

    public static long getHoldTime(){
        int[] param = {Statistics.HOLD_TIME, Statistics.NUM_TAKEN_LOCKS};
        double[] values = getParameters(param);
        double numLocks = values[1];
        double holdTime = values[0];
        if(numLocks!=0)
            return (long)(holdTime/numLocks);
        return 0L;
    }

    public static double getTransactionsWritePercentage(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getTransactionsWritePercentage();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.NUM_STARTED_WRITE_TX, Statistics.NUM_STARTED_TX_RO, Statistics.NUM_STARTED_TX_WR};
            double[] values = getParameters(param);
            double writeTX = values[0];
            double totalTX = values[1] + values[2];
            if(totalTX!=0){
                return writeTX / totalTX;
            }
            return 0;
        }
    }

    public static double getCommittedTransactionsWritePercentage(){
        if(collapseBeforeQuery){
            NodeStats stats= collapseStatistics();
            //return stats.getCommittedTransactionsWritePercentage();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.NUM_COMMITTED_TX_RO, Statistics.NUM_COMMITTED_TX_WR};
            double[] values = getParameters(param);
            double numCommits = values[1];
            double numTotal = values[1]+values[0];
            if(numTotal!=0){
                return numCommits / numTotal;
            }
            return 0D;
        }
    }

    public static long getWaitingTimeOnLock(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getWaitingTimeOnLock();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.WAITING_TIME_ON_LOCKS, Statistics.NUM_WAITED_FOR_LOCKS};
            double[] values = getParameters(param);
            double waitingTime = values[0];
            double waitedLocks = values[1];
            if(waitedLocks!=0){
                return (long)(waitingTime / waitedLocks);
            }
            return 0L;
        }
    }

    public static long getRollbackCommandCost(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getRollbackCommandCost();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.ROLLBACK_COMMAND_COST, Statistics.NUM_ROLLBACKS};
            double[] values = getParameters(param);
            double rollbackCost = values[0];
            double numRollbacks =  values[1];
            if(numRollbacks!=0){
                return (long)(rollbackCost / numRollbacks);
            }
            return 0L;
        }
    }

    public static long getCommitCommandCost(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getCommitCommandCost();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.COMMIT_COMMAND_COST, Statistics.NUM_COMMITTED_TX_WR};
            double[] values = getParameters(param);
            double commitCost = values[0];
            double numCommits =  values[1];
            if(numCommits!=0){
                return (long)(commitCost / numCommits);
            }
            return 0L;
        }
    }

    public static long getClusteredGetCommandSize(){
        int[] param = {Statistics.CLUSTERED_GET_COMMAND_SIZE,Statistics.NUM_CLUSTERED_GET_COMMANDS};
        double[] values = getParameters(param);
        double size = values[0];
        double num = values[1];
        if(num!=0)
            return (long)(size / num);
        return 0L;
    }

    public static long getRemoteGetCost(){
        int[] param = {Statistics.REMOTE_GET_COST, Statistics.NUM_REMOTE_GETS};
        double[] values = getParameters(param);
        double cost = values[0];
        double num = values[1];
        if (num!=0)
            return (long)(cost / num);
        return 0L;
    }

    public static long getTimeoutExceptionsOnPrepare(){
        int[] param = {Statistics.NUM_TIMEOUT_EXCEPTION_ON_PREPARE};
        double[] values = getParameters(param);
        return (long)values[0];
    }

    public static double getNumNodesInvolvedInPrepare(){
        int[] param = {Statistics.NUM_NODES_INVOLVED_IN_PREPARE, Statistics.NUM_PREPARES};
        double[] value = getParameters(param);
        double numNodes = value[0];
        double numPrepares = value[1];
        if(numPrepares!=0)
            return numNodes / numPrepares;
        return 0D;
    }

    public static long getDeadlockExceptionsOnPrepare(){
        int[] param = {Statistics.NUM_DEADLOCK_EXCEPTION_ON_PREPARE};
        double[] values = getParameters(param);
        return (long)values[0];
    }

    public static long getLockAcquisitionRate(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            double seconds = (System.nanoTime() - windowInitTime)/ 10e9D;
            //return (long)(((double)stats.getTakenLocks() / seconds));
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.NUM_TAKEN_LOCKS};
            double[] values = getParameters(param);
            double numLocks = values[0];
            double seconds = ((double)(System.nanoTime() - windowInitTime))/ 10e9D;
            return (long)(numLocks / seconds);
        }
    }

    public static double getNumPutsPerSuccessfulTransaction(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getNumPutsPerSuccessfulTransaction();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.NUM_COMMITTED_LOCAL_LOCKS, Statistics.NUM_COMMITTED_TX_WR};
            double[] values = getParameters(param);
            double puts = values[0];
            double prepares = values[1];
            if(prepares!=0){
                return puts / prepares;
            }
            return 0D;
        }
    }

    public static double getTransactionsArrivalRate(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getTransactionsArrivalRate();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.NUM_STARTED_TX_RO, Statistics.NUM_STARTED_TX_WR};
            double[] values = getParameters(param);
            double numTX = values[0] + values[1];
            double seconds = ((double)(System.nanoTime() - windowInitTime))/ 10e9D;
            return numTX / seconds;
        }
    }

    public static double getApplicationContentionFactor(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.NUM_TAKEN_LOCKS, Statistics.NUM_PUTS_ON_LOCAL_KEY, Statistics.NUM_PUTS_ON_REMOTE_KEY, Statistics.NUM_LOCAL_LOCAL_CONFLICTS, Statistics.NUM_LOCAL_REMOTE_CONFLICTS,Statistics.HOLD_TIME};
            double[] values = getParameters(param);
            double takenLocks = values[0];
            double puts = values[1]+values[2];
            double confl = values[3] + values[4];
            double hold = values[5];
            double pCont = confl / puts;
            double nanos = ((double)(System.nanoTime() - windowInitTime));
            double lockArrival = takenLocks / nanos;
            double utilization = lockArrival * hold;
            if(utilization!=0){
                return pCont / utilization;
            }
            return 0D;

        }
    }

    public static long getAvgRemoteExec(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            //return stats.getAvgRemoteExec();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
        }
        else{
            int[] param = {Statistics.AVG_REMOTE_EXEC, Statistics.NUM_RTTS};
            double[] values = getParameters(param);
            double exec = values[0];
            double commits = values[1];
            if(commits!=0){
                return (long)(exec / commits);
            }
            return 0L;

        }
    }

    public static long getMaxRemoteExec(){
        if(collapseBeforeQuery){
            NodeStats stats = collapseStatistics();
            throw new RuntimeException("CollapseBeforeQuery not supported yet");
            //return stats.getMaxRemoteExec();
        }
        else{
            int[] param = {Statistics.MAX_REMOTE_EXEC, Statistics.NUM_RTTS};
            double[] values = getParameters(param);
            double exec = values[0];
            double commits = values[1];
            if(commits!=0){
                return (long)(exec / commits);
            }
            return 0L;

        }
    }

    public static long getThroughput(){
        if(collapseBeforeQuery){
            throw new RuntimeException("Thoughput not supported yet");
        }
        else{
            int[] param = {Statistics.NUM_COMMITTED_TX_RO, Statistics.NUM_COMMITTED_TX_WR};
            double[] values = getParameters(param);
            double commits = values[0] + values[1];
            double seconds = ((double)(System.nanoTime() - windowInitTime))/ 10e9D;
            return (long) (commits / seconds);
        }
    }

    public static Map<Long,Long> getInterArrivalHistogram(){
        return interArrivalHistogram.getInterArrivalHistogram();
    }

    public static void insertInterArrivalSample(long time, Object key){
        interArrivalHistogram.insertSample(time,key);
    }

    public static long getLocalRollbacks(){
        int[] param = {Statistics.NUM_ROLLBACKS};
        double[] values = getParameters(param);
        return (long) values[0];
    }

    public static long getLocalPuts() {
        return (long) getParameters(new int[] {Statistics.NUM_PUTS_ON_LOCAL_KEY})[0];
    }

    public static long getRemotePuts() {
        return (long) getParameters(new int[] {Statistics.NUM_PUTS_ON_REMOTE_KEY})[0];
    }

    public static long getLocalGets() {
        return (long) getParameters(new int[] {Statistics.NUM_GETS_ON_LOCAL_KEY})[0];
    }

    public static long getRemoteGets() {
        return (long) getParameters(new int[] {Statistics.NUM_GETS_ON_REMOTE_KEY})[0];
    }

    private synchronized static NodeStats collapseStatistics(){
        NodeStats stat = new NodeStats();
        Iterator<ISPNStats> it = statsList.iterator();
        ISPNStats temp;
        while(it.hasNext()){
            temp = it.next();
            //Not thread-safe
            mergeAndFlush(stat, temp);
            //if it's a threadStats AND its thread is dead, then I can remove it (without sync)
            if(it instanceof ThreadStatistics){
                ThreadStatistics ts = (ThreadStatistics) temp;
                if(ts.getThread_state()==State.TERMINATED)
                    statsList.remove(ts);

            }
            //if it's a previously aggregated node, I can remove it after having copied its content
            else{
                statsList.remove(temp);
            }
        }
        //In the end, the just created node becomes the first in the list
        statsList.add(0,stat);
        return stat;
    }

    private static void mergeAndFlush(ISPNStats dest, ISPNStats src){

        for(int i=0; i< Statistics.NUM_STATS;i++){
            dest.addParameter(i, src.getParameter(i));
        }
        src.reset();
    }

    private static double[] getParameters(int[] indexes){
        Iterator<ISPNStats> it= statsList.iterator();
        int length = indexes.length;
        double[] stats = new double[length];
        ISPNStats temp;
        int i = 0;
        while(it.hasNext()){
            temp = it.next();
            while(i<length){
                stats[i]+= temp.getParameter(indexes[i]);
                i++;
            }
            i=0;
        }
        return stats;
    }
    /*
    private static List<Object> getObjects(int index){
        Iterator<ISPNStats> it= statsList.iterator();
        List<Object> ret = new LinkedList<Object>();
        ISPNStats temp;
        int i = 0;
        while(it.hasNext()){
            temp = it.next();
            ret.add(temp.getObjects(index));
        }
        return ret;

    }
    */


}