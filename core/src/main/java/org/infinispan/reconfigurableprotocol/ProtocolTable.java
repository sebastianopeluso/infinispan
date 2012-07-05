package org.infinispan.reconfigurableprotocol;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;

import javax.transaction.Transaction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * keeps the protocol ID of the replication protocol used to execute each local transaction 
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ProtocolTable {

   private final ConcurrentMap<Transaction, String> transactionToProtocol;
   private ReconfigurableReplicationManager manager;

   public ProtocolTable() {
      transactionToProtocol = new ConcurrentHashMap<Transaction, String>();
   }

   @Inject
   public void inject(ReconfigurableReplicationManager manager) {
      this.manager = manager;
   }

   /**
    * returns the protocol Id to run the transaction. If it is the first time, it maps the transaction to the current
    * protocol Id
    *
    * @param transaction   the transaction
    * @return              the protocol Id to run the transaction
    */
   public final String getProtocolId(Transaction transaction) {
      String currentProtocolId = manager.getCurrentProtocolId();
      String existing = transactionToProtocol.putIfAbsent(transaction, currentProtocolId);
      if (existing != null) {
         return existing;
      }
      return currentProtocolId;
   }

   /**
    * remove the transaction
    * @param transaction   the transaction
    */
   public final void remove(Transaction transaction) {
      transactionToProtocol.remove(transaction);
   }

}