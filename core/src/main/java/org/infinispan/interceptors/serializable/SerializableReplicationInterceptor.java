package org.infinispan.interceptors.serializable;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Map;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class SerializableReplicationInterceptor extends ReplicationInterceptor {

   private static final Log log = LogFactory.getLog(SerializableReplicationInterceptor.class);

   private VersionVCFactory versionVCFactory;
   private CommandsFactory commandsFactory;

   @Inject
   public void inject(VersionVCFactory versionVCFactory, CommandsFactory commandsFactory){
      this.versionVCFactory = versionVCFactory;
      this.commandsFactory = commandsFactory;
   }

   @Override
   protected PrepareCommand buildPrepareCommandForResend(TxInvocationContext ctx, CommitCommand command) {
      // Make sure this is 1-Phase!! this should never happen, right?
      return commandsFactory.buildSerializablePrepareCommand(command.getGlobalTransaction(), ctx.getModifications(), true);
   }

   @Override
   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      Map<Address, Response> responses = rpcManager.invokeRemotely(null, command, true, false, false);
      log.debugf("broadcast prepare command for transaction %s. responses are: %s",
                 command.getGlobalTransaction().prettyPrint(), responses.toString());

      joinVersions(responses.values(), context);
   }

   private void joinVersions(Collection<Response> responses, TxInvocationContext ctx) {
      VersionVC allPreparedVC = versionVCFactory.createVersionVC();
      ctx.setCommitVersion(allPreparedVC);
      GlobalTransaction gtx = ctx.getGlobalTransaction();

      if (responses.isEmpty()) {         
         return;
      }
            
      //process all responses
      for (Response r : responses) {
         if (r instanceof SuccessfulResponse) {
            VersionVC preparedVC = (VersionVC) ((SuccessfulResponse) r).getResponseValue();
            allPreparedVC.setToMaximum(preparedVC);
            log.debugf("[%s] received response %s. all vector clock together is %s", gtx.prettyPrint(), preparedVC, allPreparedVC);
         } else if(r instanceof ExceptionResponse) {
            log.debugf("[%s] received a negative response %s", gtx.prettyPrint(),r);
            throw new RpcException(((ExceptionResponse) r).getException());
         } else if(!r.isSuccessful()) {
            log.debugf("[%s] received a negative response %s", gtx.prettyPrint(), r);
            throw new CacheException("Unsuccessful response received... aborting transaction " + gtx.prettyPrint());
         }
      }      
   }
}
