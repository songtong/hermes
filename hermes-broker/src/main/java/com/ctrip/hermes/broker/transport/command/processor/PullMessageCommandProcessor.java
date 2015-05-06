package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.longpolling.LongPollingService;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

public class PullMessageCommandProcessor implements CommandProcessor {

	@Inject
	private LongPollingService m_longPollingService;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_PULL);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		PullMessageCommand reqCmd = (PullMessageCommand) ctx.getCommand();
		// TODO validate topic, partition, check if this broker is the leader of the topic-partition

		long correlationId = reqCmd.getHeader().getCorrelationId();

		m_longPollingService.schedulePush(new Tpg(reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId()),
		      correlationId, reqCmd.getSize(), ctx.getChannel(), reqCmd.getExpireTime());
	}
}
