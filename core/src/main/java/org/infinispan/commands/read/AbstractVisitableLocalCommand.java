package org.infinispan.commands.read;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.jgroups.blocks.MessageRequest;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractVisitableLocalCommand extends AbstractLocalCommand implements VisitableCommand {

   private MessageRequest messageRequest;

   @Override
   public void setMessageRequest(MessageRequest request, ResponseGenerator responseGenerator) {
      this.messageRequest = request;
   }

   @Override
   public void sendReply(Object reply, boolean isExceptionThrown) {
      if (messageRequest == null) {
         throw new NullPointerException("Message Request is null");
      }
      messageRequest.sendReply(reply, isExceptionThrown);
   }

}
