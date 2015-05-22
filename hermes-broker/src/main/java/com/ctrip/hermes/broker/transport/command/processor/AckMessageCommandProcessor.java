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

		for (Map.Entry<Triple<Tpp, String, Boolean>, Map<Long, Integer>> entry : cmd.getAckMsgs().entrySet()) {
			Tpp tpp = entry.getKey().getFirst();
			String groupId = entry.getKey().getMiddle();
			boolean isResend = entry.getKey().getLast();
			Map<Long, Integer> msgSeqs = entry.getValue();
			m_ackManager.acked(tpp, groupId, isResend, msgSeqs);

			if (log.isDebugEnabled()) {
				log.debug("Client acked(topic={}, partition={}, pirority={}, groupId={}, isResend={}, msgIds={})",
				      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, msgSeqs.keySet());
			}
		}

		for (Map.Entry<Triple<Tpp, String, Boolean>, Map<Long, Integer>> entry : cmd.getNackMsgs().entrySet()) {
			Tpp tpp = entry.getKey().getFirst();
			String groupId = entry.getKey().getMiddle();
			boolean isResend = entry.getKey().getLast();
			Map<Long, Integer> msgSeqs = entry.getValue();
			m_ackManager.nacked(tpp, groupId, isResend, msgSeqs);

			if (log.isDebugEnabled()) {
				log.debug("Client nacked(topic={}, partition={}, pirority={}, groupId={}, isResend={}, msgIds={})",
				      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, msgSeqs.keySet());
			}
		}
	}
}
