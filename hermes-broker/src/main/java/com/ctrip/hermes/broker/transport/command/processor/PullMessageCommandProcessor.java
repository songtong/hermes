package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.longpolling.LongPollingService;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.PullMessageAckCommand;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

public class PullMessageCommandProcessor implements CommandProcessor {

	@Inject
	private LongPollingService m_longPollingService;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfig m_config;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_PULL);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		PullMessageCommand reqCmd = (PullMessageCommand) ctx.getCommand();
		long correlationId = reqCmd.getHeader().getCorrelationId();

		Lease lease = m_leaseContainer.acquireLease(reqCmd.getTopic(), reqCmd.getPartition(), m_config.getSessionId());

		if (lease != null) {
			m_longPollingService.schedulePush(new Tpg(reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId()),
			      correlationId, reqCmd.getSize(), ctx.getChannel(), reqCmd.getExpireTime(), lease);
		} else {
			// can not acquire lease, response with empty result
			PullMessageAckCommand cmd = new PullMessageAckCommand();
			cmd.getHeader().setCorrelationId(reqCmd.getHeader().getCorrelationId());

			ctx.getChannel().writeCommand(cmd);
		}
	}
}