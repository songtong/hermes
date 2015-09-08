package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.longpolling.LongPollingService;
import com.ctrip.hermes.broker.longpolling.PullMessageTask;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;

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
		return Arrays.asList(CommandType.MESSAGE_PULL, CommandType.MESSAGE_PULL_V2);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		int version = ctx.getCommand().getHeader().getVersion();
		PullMessageTask task = 1 == version ? preparePullMessageTask(ctx) : preparePullMessageTaskV2(ctx);
		Tpg tpg = task.getTpg();

		try {
			if (m_metaService.containsConsumerGroup(tpg.getTopic(), tpg.getGroupId())) {
				Lease lease = m_leaseContainer.acquireLease(tpg.getTopic(), tpg.getPartition(), m_config.getSessionId());
				if (lease != null) {
					task.setBrokerLease(lease);
					m_longPollingService.schedulePush(task);
					return;
				} else {
					logDebug("No broker lease to handle client pull message reqeust.", task.getCorrelationId(), tpg, null);
				}
			} else {
				logDebug("Consumer group not found for topic.", task.getCorrelationId(), tpg, null);
			}
		} catch (Exception e) {
			logDebug("Exception occurred while handling client pull message reqeust.", task.getCorrelationId(), tpg, e);
		}
		// can not acquire lease, response with empty result
		responseError(task);
	}

	private void logDebug(String debugInfo, long correlationId, Tpg tpg, Exception e) {
		if (log.isDebugEnabled()) {
			if (e != null) {
				log.debug(debugInfo + " [correlation id: {}] {}", correlationId, tpg, e);
			} else {
				log.debug(debugInfo + " [correlation id: {}] {}", correlationId, tpg);
			}
		}
	}

	private void responseError(PullMessageTask task) {
		Command cmd = null;
		switch (task.getPullMessageCommandVersion()) {
		case 1:
			cmd = new PullMessageResultCommand();
		case 2:
			cmd = new PullMessageResultCommandV2();
		}
		cmd.getHeader().setCorrelationId(task.getCorrelationId());
		task.getChannel().writeAndFlush(cmd);
	}

	private PullMessageTask preparePullMessageTask(CommandProcessorContext ctx) {
		PullMessageCommand cmd = (PullMessageCommand) ctx.getCommand();
		PullMessageTask task = new PullMessageTask();

		task.setPullCommandVersion(1);

		task.setBatchSize(cmd.getSize());
		task.setChannel(ctx.getChannel());
		task.setCorrelationId(cmd.getHeader().getCorrelationId());
		task.setExpireTime(cmd.getExpireTime());
		task.setTpg(new Tpg(cmd.getTopic(), cmd.getPartition(), cmd.getGroupId()));

		return task;
	}

	private PullMessageTask preparePullMessageTaskV2(CommandProcessorContext ctx) {
		PullMessageCommandV2 cmd = (PullMessageCommandV2) ctx.getCommand();
		PullMessageTask task = new PullMessageTask();

		task.setPullCommandVersion(2);

		task.setBatchSize(cmd.getSize());
		task.setChannel(ctx.getChannel());
		task.setCorrelationId(cmd.getHeader().getCorrelationId());
		task.setExpireTime(cmd.getExpireTime());
		task.setTpg(new Tpg(cmd.getTopic(), cmd.getPartition(), cmd.getGroupId()));

		task.setStartOffset(cmd.getOffset());
		task.setWithOffset(PullMessageCommandV2.PULL_WITH_OFFSET == cmd.getPullType());

		return task;
	}
}
