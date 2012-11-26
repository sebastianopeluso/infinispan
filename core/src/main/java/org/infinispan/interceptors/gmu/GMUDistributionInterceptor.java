package org.infinispan.interceptors.gmu;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DistributionInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.transaction.gmu.GMUHelper.joinAndSetTransactionVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUDistributionInterceptor extends DistributionInterceptor {

   private static final Log log = LogFactory.getLog(GMUDistributionInterceptor.class);

   private GMUVersionGenerator versionGenerator;

   @Inject
   public void setVersionGenerator(VersionGenerator versionGenerator) {
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      //we need to join the read set owners to validate
      Set<Address> realRecipients = new HashSet<Address>(recipients);
      for (List<Address> list : dm.locateAll(ctx.getReadSet()).values()) {
         realRecipients.addAll(list);
      }      

      Map<Address, Response> responses = rpcManager.invokeRemotely(recipients, command, true, false, false);
      log.debugf("prepare command for transaction %s is sent. responses are: %s",
                 command.getGlobalTransaction().prettyPrint(), responses.toString());

      joinAndSetTransactionVersion(responses.values(), ctx, versionGenerator);
   }
}
