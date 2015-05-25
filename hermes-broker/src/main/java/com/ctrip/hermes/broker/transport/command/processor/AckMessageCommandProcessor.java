package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.ack.AckManager;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.AckMessageCommand;
import com.ctrip.hermes.core.transport.command.AckMessageCommand.AckContext;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckMessageCommandProcessor implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(AckMessageCommandProcessor.class);

	@Inject
	private AckManager m_ackManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_ACK);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		AckMessageCommand cmd = (AckMessageCommand) ctx.getCommand();

		for (Map.Entry<Triple<Tpp, String, Boolean>, List<AckContext>> entry : cmd.getAckMsgs().entrySet()) {
			Tpp tpp = entry.getKey().getFirst();
			String groupId = entry.getKey().getMiddle();
			boolean isResend = entry.getKey().getLast();
			List<AckContext> ackContexts = entry.getValue();
			m_ackManager.acked(tpp, groupId, isResend, ackContexts);

			if (log.isDebugEnabled()) {
				log.debug("Client acked(topic={}, partition={}, pirority={}, groupId={}, isResend={}, contexts={})",
				      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, ackContexts);
			}
		}

		for (Map.Entry<Triple<Tpp, String, Boolean>, List<AckContext>> entry : cmd.getNackMsgs().entrySet()) {
			Tpp tpp = entry.getKey().getFirst();
			String groupId = entry.getKey().getMiddle();
			boolean isResend = entry.getKey().getLast();
			List<AckContext> nackContexts = entry.getValue();
			m_ackManager.nacked(tpp, groupId, isResend, nackContexts);

			if (log.isDebugEnabled()) {
				log.debug("Client nacked(topic={}, partition={}, pirority={}, groupId={}, isResend={}, contexts={})",
				      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, nackContexts);
			}
		}
	}
}
