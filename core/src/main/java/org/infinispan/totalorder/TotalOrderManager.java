package org.infinispan.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.totalOrder.TotalOrderRemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * This class is responsible to validate transactions in the total order based protocol. It ensures the delivered order
 * and will validate multiple transactions in parallel if they are non conflicting transaction.
 *
 * @author Pedro Ruivo
 * @author mircea.markus@jboss.com
 * @since 5.2.0
 */
public interface TotalOrderManager {

   /**
    * Put the transaction in the validation queue for further validation. Transactions can be validated in parallel if
    * it is possible.
    */
   void validateTransaction(PrepareCommand prepareCommand, TxInvocationContext ctx, CommandInterceptor invoker);

   /**
    * This will mark a global transaction as finished. It will be invoked in the processing of the commit command in
    * repeatable read with write skew (not implemented yet!)
    */
   void finishTransaction(GlobalTransaction gtx, boolean ignoreNullTxInfo);

   /**
    * This ensures the order between the commit/rollback commands and the prepare command.
    * <p/>
    * However, if the commit/rollback command is deliver first, then they don't need to wait until the prepare is
    * deliver. The mark the remote transaction for commit or rollback and when the prepare arrives, it adapts its
    * behaviour: -> if it must rollback, the prepare is discarded (no needing for processing) -> if it must commit, then
    * it sets the one phase flag and wait for this turn, committing the modifications and it skips the write skew check
    * (note: the commit command saves the new versions in remote transaction)
    * <p/>
    * If the prepare is already in process, then the commit/rollback is blocked until the validation is finished.
    *
    * @param commit            true if it is a commit command, false if it is a rollback command
    * @return true if the command needs to be processed, false otherwise
    */
   boolean waitForTxPrepared(TotalOrderRemoteTransaction remoteTransaction, boolean commit, EntryVersionsMap newVersions);

   /**
    * Remove the keys from the map (if their didn't change) and release the count down latch, unblocking the next
    * transaction
    */
   void finishTransaction(TotalOrderRemoteTransaction remoteTransaction);

   /**
    * Adds a local transaction to the map. Later, it will be notified when the modifications are applied in the data
    * container
    */
   void addLocalTransaction(GlobalTransaction globalTransaction, LocalTransaction localTransaction);

   void waitForPrepareToSucceed(TxInvocationContext context);
}
