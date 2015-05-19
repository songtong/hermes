package com.ctrip.hermes.broker.longpolling;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.queue.MessageQueueCursor;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = LongPollingService.class)
public class DefaultLongPollingService extends AbstractLongPollingService implements
      Initializable {

	private ScheduledExecutorService m_scheduledThreadPool;

	@Override
	public void initialize() throws InitializationException {
		m_scheduledThreadPool = Executors.newScheduledThreadPool(m_config.getLongPollingServiceThreadCount(),
		      HermesThreadFactory.create(m_config.getBackgroundThreadGroup(), "LongPollingService", false));
	}

	@Override
	public void schedulePush(Tpg tpg, long correlationId, int batchSize, EndpointChannel channel, long expireTime,
	      Lease brokerLease) {
		final PullMessageTask pullMessageTask = new PullMessageTask(tpg, correlationId, batchSize, channel, expireTime,
		      brokerLease);
		m_scheduledThreadPool.submit(new Runnable() {

			@Override
			public void run() {
				executeTask(pullMessageTask);
			}

		});
	}

	private void executeTask(final PullMessageTask pullMessageTask) {
		// skip expired task
		if (pullMessageTask.getExpireTime() < m_systemClockService.now()) {
			return;
		}

		if (pullMessageTask.getBrokerLease().getExpireTime() >= m_systemClockService.now()) {
			if (!queryAndResponseData(pullMessageTask)) {
				m_scheduledThreadPool.schedule(new Runnable() {

					@Override
					public void run() {
						executeTask(pullMessageTask);
					}
				}, m_config.getLongPollingCheckInterval(), TimeUnit.MILLISECONDS);
			}
		} else {
			// no lease, return empty cmd
			response(pullMessageTask, null);
		}
	}

	private boolean queryAndResponseData(PullMessageTask pullTask) {
		Tpg tpg = pullTask.getTpg();

		MessageQueueCursor cursor = m_queueManager.getCursor(tpg, pullTask.getBrokerLease());

		List<TppConsumerMessageBatch> batches = null;

		batches = cursor.next(pullTask.getBatchSize());

		if (batches != null && !batches.isEmpty()) {
			// notify ack manager
			for (TppConsumerMessageBatch batch : batches) {
				m_ackManager.delivered(new Tpp(batch.getTopic(), batch.getPartition(), batch.isPriority()),
				      tpg.getGroupId(), batch.isResend(), batch.getMsgSeqs());
			}

			response(pullTask, batches);
			return true;
		} else {
			return false;
		}
	}
}
