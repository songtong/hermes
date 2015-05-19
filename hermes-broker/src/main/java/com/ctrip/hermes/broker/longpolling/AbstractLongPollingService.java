package com.ctrip.hermes.broker.longpolling;

import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.ack.AckManager;
import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.PullMessageAckCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractLongPollingService implements LongPollingService {
	@Inject
	protected MessageQueueManager m_queueManager;

	@Inject
	protected AckManager m_ackManager;

	@Inject
	protected BrokerConfig m_config;

	@Inject
	protected SystemClockService m_systemClockService;

	protected void response(PullMessageTask pullTask, List<TppConsumerMessageBatch> batches) {
		PullMessageAckCommand cmd = new PullMessageAckCommand();
		if (batches != null) {
			cmd.addBatches(batches);
		}
		cmd.getHeader().setCorrelationId(pullTask.getCorrelationId());

		pullTask.getChannel().writeCommand(cmd);
	}
}
