package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.longpolling.LongPollingService;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

public class PullMessageCommandProcessor implements CommandProcessor {

	private static final Logger log = LoggerFactory.getLogger(PullMessageCommandProcessor.class);

	@Inject
	private LongPollingService m_longPollingService;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfig m_config;

	@Inject
	private MetaService m_metaService;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_PULL);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		PullMessageCommand reqCmd = (PullMessageCommand) ctx.getCommand();
		long correlationId = reqCmd.getHeader().getCorrelationId();

		try {
			if (m_metaService.containsConsumerGroup(reqCmd.getTopic(), reqCmd.getGroupId())) {
				Lease lease = m_leaseContainer.acquireLease(reqCmd.getTopic(), reqCmd.getPartition(),
				      m_config.getSessionId());

				if (lease != null) {
					m_longPollingService.schedulePush(
					      new Tpg(reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId()), correlationId,
					      reqCmd.getSize(), ctx.getChannel(), reqCmd.getExpireTime(), lease);
					return;
				} else {
					if (log.isDebugEnabled()) {
						log.debug(
						      "No broker lease to handle client pull message reqeust(correlationId={}, topic={}, partition={}, groupId={})",
						      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId());
					}
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Consumer group not found for topic (correlationId={}, topic={}, partition={}, groupId={})",
					      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId());
				}
			}
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug(
				      "Exception occurred while handling client pull message reqeust(correlationId={}, topic={}, partition={}, groupId={})",
				      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId(), e);
			}
		}
		// can not acquire lease, response with empty result
		PullMessageResultCommand cmd = new PullMessageResultCommand();
		cmd.getHeader().setCorrelationId(reqCmd.getHeader().getCorrelationId());

		ctx.getChannel().writeAndFlush(cmd);
	}
}
