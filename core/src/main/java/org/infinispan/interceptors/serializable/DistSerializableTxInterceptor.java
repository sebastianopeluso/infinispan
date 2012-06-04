package org.infinispan.interceptors.serializable;

import org.infinispan.CacheException;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.SerializablePrepareCommand;
import org.infinispan.container.key.ContextAwareKey;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.mvcc.CommitLog;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistSerializableTxInterceptor extends SerializableTxInterceptor {

   private static final Log log = LogFactory.getLog(DistSerializableTxInterceptor.class);

   private CommitLog commitLog;
   private DistributionManager distributionManager;

   @Inject
   public void inject(CommitLog commitLog, DistributionManager distributionManager) {
      this.commitLog = commitLog;
      this.distributionManager = distributionManager;
   }

   @Override
   protected void initializeSerializablePrepareCommand(TxInvocationContext ctx, SerializablePrepareCommand command) {
      command.setReadSet(((LocalTransaction)ctx.getCacheTransaction()).getRemoteReadSet());
      command.setVersion(ctx.getPrepareVersion());
   }

   @Override
   protected Object afterSuccessfullyPrepared(TxInvocationContext ctx, PrepareCommand command, Object retVal) {
      boolean info = log.isInfoEnabled();
      boolean debug = log.isDebugEnabled();
      VersionVC prepareVC;

      if(hasLocalKeys(command.getAffectedKeys())) {
         prepareVC = commitQueue.addTransaction(command.getGlobalTransaction(), ctx.calculateVersionToRead(versionVCFactory),
                                               ctx.clone());

         if(info) {
            log.infof("Transaction %s can commit. It was added to commit queue. " +
                            "The 'temporary' commit version is %s", command.getGlobalTransaction().prettyPrint(), prepareVC);
         }
      } else {
         //if it is readonly, return the most recent version
         prepareVC = commitLog.getActualVersion();

         if(info) {
            log.infof("Transaction %s can commit. It is read-only on this node. " +
                            "The 'temporary' commit version is %s", command.getGlobalTransaction().prettyPrint(), prepareVC);
         }
      }

      VersionVC commitVC;
      if(ctx.isOriginLocal()) {
         //the retVal has the maximum vector clock of all involved nodes
         //this in only performed in the node that executed the transaction
         commitVC = ((LocalTransaction)ctx.getCacheTransaction()).getCommitVersion();         
         commitVC.setToMaximum(prepareVC);
         calculateCommitVC(commitVC, getVCPositions(getWriteSetMembers(command.getAffectedKeys())));
      } else {
         commitVC = prepareVC;               
      }

      if(debug) {
         log.debugf("Transaction [%s] %s commit version is %s", command.getGlobalTransaction().prettyPrint(),
                    (ctx.isOriginLocal() ? "final" : "temporary"), commitVC);
      }
      return commitVC;
   }

   @Override
   protected Object afterSuccessfullyRead(InvocationContext ctx, GetKeyValueCommand command, Object valueRead) {
      Object retVal = valueRead;
      if(ctx.isInTxScope() && ctx.isOriginLocal()) {
         TxInvocationContext tctx = (TxInvocationContext) ctx;

         //update vc
         InternalMVCCEntry ime;
         Object key = command.getKey();
         if(distributionManager.getLocality(key).isLocal()){
            //We have read on this node, The key of the read value is found in the local read set
            ime = ctx.getLocalReadKey(key);

            if((key instanceof ContextAwareKey) && ((ContextAwareKey)key).identifyImmutableValue()){
               //The key identifies an immutable object. We don't need validation for this key.
               ctx.removeLocalReadKey(key);
            }
         } else {
            //We have read on a remote node. The key of the read value is found in the remote read set
            ime = ctx.getRemoteReadKey(key);

            if((key instanceof ContextAwareKey) && ((ContextAwareKey)key).identifyImmutableValue()){
               //The key identifies an immutable object. We don't need validation for this key.
               ctx.removeRemoteReadKey(key);
            }
         }

         if(ime == null) {
            //String gtxID = Util.prettyPrintGlobalTransaction(txctx.getGlobalTransaction());
            //log.warnf("InternalMVCCEntry is null for key %s in transaction %s", command.getKey(), gtxID);
         } else if(tctx.hasModifications() && !ime.isMostRecent()) {
            //read an old value... the tx will abort in commit,
            //so, do not waste time and abort it now            
            throw new CacheException("Read-Write Transaction read an old value");
         } else {
            VersionVC v = ime.getVersion();
            tctx.updateVectorClock(v);
         }
      }

      //remote get
      if(!ctx.isOriginLocal() && ctx.readBasedOnVersion()) {
         retVal = ctx.getLocalReadKey(command.getKey());
         ctx.removeLocalReadKey(command.getKey()); //We don't need a readSet on a remote node!
      }

      return retVal;
   }

   private Set<Address> getWriteSetMembers(Collection<Object> writeSet) {
      Set<Address> members = new HashSet<Address>(writeSet.size());
      for (Collection<Address> addresses : distributionManager.locateAll(writeSet).values()) {
         members.addAll(addresses);
      }
      return members;
   }

   private Integer[] getVCPositions(Collection<Address> writeSetMembers) {
      Set<Integer> positions = new HashSet<Integer>();
      for(Address address : writeSetMembers) {
         positions.add(distributionManager.getAddressID(address));
      }

      Integer[] retVal = positions.toArray(new Integer[positions.size()]);

      if(log.isDebugEnabled()) {
         log.debugf("Address IDs for %s are %s", writeSetMembers, positions);
      }

      return retVal;
   }

   private void calculateCommitVC(VersionVC vc, Integer[] writeGroups) {
      //first, calculate the maximum value in the write groups
      String originalVC = null;
      if(log.isDebugEnabled()) {
         originalVC = vc.toString();
      }

      long maxValue = 0;
      for(Integer pos : writeGroups) {
         long val = this.versionVCFactory.translateAndGet(vc,pos);

         if(val > maxValue) {
            maxValue = val;
         }
      }

      //second, set the write groups position to the maximum
      for(Integer pos : writeGroups) {
         this.versionVCFactory.translateAndSet(vc,pos,maxValue);

      }

      if(log.isDebugEnabled()) {
         log.debugf("Calculate the commit version from %s. The modified groups are %s and the final" +
                          " version is %s", originalVC, writeGroups, vc);
      }
   }

   private boolean hasLocalKeys(Collection<Object> keys) {
      for (Object key : keys) {
         if (distributionManager.getLocality(key).isLocal()) {
            return true;
         }
      }
      return false;
   }
}
