package com.ctrip.hermes.broker.transport.command.processor;

import io.netty.channel.Channel;

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
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v3.PullMessageCommandV3;
import com.ctrip.hermes.core.transport.command.v3.PullMessageResultCommandV3;

public class PullMessageCommandProcessorV3 implements CommandProcessor {

	private static final Logger log = LoggerFactory.getLogger(PullMessageCommandProcessorV3.class);

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
		return Arrays.asList(CommandType.MESSAGE_PULL_V3);
	}

	@Override
	public void process(CommandProcessorContext ctx) {

		PullMessageCommandV3 reqCmd = (PullMessageCommandV3) ctx.getCommand();

		long correlationId = reqCmd.getHeader().getCorrelationId();

		try {
			if (m_metaService.containsConsumerGroup(reqCmd.getTopic(), reqCmd.getGroupId())) {
				Lease lease = m_leaseContainer.acquireLease(reqCmd.getTopic(), reqCmd.getPartition(),
				      m_config.getSessionId());
				if (lease != null) {

					PullMessageTask task = createPullMessageTask(reqCmd, lease, ctx.getChannel(), ctx.getRemoteIp());
					m_longPollingService.schedulePush(task);
					return;
				} else {
					log.debug(
					      "No broker lease to handle client pull message reqeust(correlationId={}, topic={}, partition={}, groupId={})",
					      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId());
				}
			} else {
				log.debug("Consumer group not found for topic (correlationId={}, topic={}, partition={}, groupId={})",
				      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId());
			}
		} catch (Exception e) {
			log.debug(
			      "Exception occurred while handling client pull message reqeust(correlationId={}, topic={}, partition={}, groupId={})",
			      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId(), e);
		}

		// can not acquire lease, response with empty result
		PullMessageResultCommandV3 cmd = new PullMessageResultCommandV3();
		cmd.getHeader().setCorrelationId(reqCmd.getHeader().getCorrelationId());
		cmd.setBrokerAccepted(false);

		ctx.getChannel().writeAndFlush(cmd);

	}

	private PullMessageTask createPullMessageTask(PullMessageCommandV3 cmd, Lease brokerLease, Channel channel,
	      String clientIp) {
		PullMessageTask task = new PullMessageTask();

		task.setBatchSize(cmd.getSize());
		task.setBrokerLease(brokerLease);
		task.setChannel(channel);
		task.setCorrelationId(cmd.getHeader().getCorrelationId());
		task.setExpireTime(cmd.getExpireTime());
		task.setPullCommandVersion(3);
		task.setWithOffset(true);
		task.setStartOffset(cmd.getOffset());
		task.setTpg(new Tpg(cmd.getTopic(), cmd.getPartition(), cmd.getGroupId()));
		task.setClientIp(clientIp);

		return task;
	}
}
