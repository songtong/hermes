package com.ctrip.hermes.broker.longpolling;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;
import com.ctrip.hermes.core.transport.command.v3.PullMessageResultCommandV3;
import com.ctrip.hermes.core.transport.command.v4.PullMessageResultCommandV4;

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
			cmd = new PullMessageResultCommandV2();
			if (batches != null) {
				((PullMessageResultCommandV2) cmd).addBatches(batches);
			}
			((PullMessageResultCommandV2) cmd).setOffset(offset);
			((PullMessageResultCommandV2) cmd).setBrokerAccepted(true);
			break;
		case 3:
			cmd = new PullMessageResultCommandV3();
			if (batches != null) {
				((PullMessageResultCommandV3) cmd).addBatches(batches);
			}
			((PullMessageResultCommandV3) cmd).setOffset(offset);
			((PullMessageResultCommandV3) cmd).setBrokerAccepted(true);
			break;
		case 4:
		default:
			cmd = new PullMessageResultCommandV4();
			if (batches != null) {
				((PullMessageResultCommandV4) cmd).addBatches(batches);
			}
			((PullMessageResultCommandV4) cmd).setOffset(offset);
			((PullMessageResultCommandV4) cmd).setBrokerAccepted(true);
			break;
		}
		cmd.getHeader().setCorrelationId(pullTask.getCorrelationId());

		ChannelUtils.writeAndFlush(pullTask.getChannel(), cmd);
	}

	@Override
	public void stop() {
		if (m_stopped.compareAndSet(false, true)) {
			doStop();
		}
	}

	protected abstract void doStop();
}
