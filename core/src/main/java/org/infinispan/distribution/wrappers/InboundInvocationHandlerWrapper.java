package org.infinispan.distribution.wrappers;

import org.apache.log4j.Logger;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.stats.TransactionsStatisticsRegistry;

import javax.swing.plaf.metal.MetalBorders;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
public class InboundInvocationHandlerWrapper implements InboundInvocationHandler {

   private final InboundInvocationHandler actual;
   Logger log = Logger.getLogger(InboundInvocationHandlerWrapper.class);


   public InboundInvocationHandlerWrapper(InboundInvocationHandler actual) {
      this.actual = actual;
   }

   @Override
   public Response handle(CacheRpcCommand command, Address origin) throws Throwable {
      System.out.println("InboundInvocationHandlerWrapper.handle "+command);
      long currTime = System.nanoTime();
      Response ret;
      if(command instanceof PrepareCommand){
         try{
            ret = actual.handle(command,origin);
            TransactionsStatisticsRegistry.initRemoteTransaction();
            TransactionsStatisticsRegistry.addValue(IspnStats.REPLAY_TIME,System.nanoTime() - currTime);
            TransactionsStatisticsRegistry.incrementValue(IspnStats.REPLAYED_TXS);
            return ret;
         }
         catch(Throwable t){
            throw  t;
         }
      }
      return actual.handle(command,origin);
   }
}
