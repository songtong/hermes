package com.ctrip.hermes.broker.longpolling;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractLongPollingService implements LongPollingService {
	@Inject
	protected MessageQueueManager m_queueManager;

	@Inject
	protected BrokerConfig m_config;

	@Inject
	protected SystemClockService m_systemClockService;

	protected AtomicBoolean m_stopped = new AtomicBoolean(false);

	protected void response(PullMessageTask pullTask, List<TppConsumerMessageBatch> batches, Offset offset) {
		Command cmd = null;
		switch (pullTask.getPullMessageCommandVersion()) {
		case 1:
			cmd = new PullMessageResultCommand();
			if (batches != null) {
				((PullMessageResultCommand) cmd).addBatches(batches);
			}
			break;
		case 2:
		default:
			cmd = new PullMessageResultCommandV2();
			if (batches != null) {
				((PullMessageResultCommandV2) cmd).addBatches(batches);
			}
			((PullMessageResultCommandV2) cmd).setOffset(offset);
			break;
		}
		cmd.getHeader().setCorrelationId(pullTask.getCorrelationId());

		pullTask.getChannel().writeAndFlush(cmd);
	}

	@Override
	public void stop() {
		if (m_stopped.compareAndSet(false, true)) {
			doStop();
		}
	}

	protected abstract void doStop();
}
