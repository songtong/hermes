package com.ctrip.hermes.broker.longpolling;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.MessageQueueCursor;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.BizLogger;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.transport.netty.NettyUtils;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = LongPollingService.class)
public class DefaultLongPollingService extends AbstractLongPollingService implements Initializable {

	@Inject
	private BizLogger m_bizLogger;

	private static final Logger log = LoggerFactory.getLogger(DefaultLongPollingService.class);

	private ScheduledExecutorService m_scheduledThreadPool;

	@Override
	public void initialize() throws InitializationException {
		m_scheduledThreadPool = Executors.newScheduledThreadPool(m_config.getLongPollingServiceThreadCount(),
		      HermesThreadFactory.create("LongPollingService", false));
	}

	@Override
	public void schedulePush(final PullMessageTask task) {
		if (log.isDebugEnabled()) {
			log.debug("Schedule push(correlation id: {}) for client: {}", task.getCorrelationId(), task.getTpg());
		}

		if (m_stopped.get() || (task.isWithOffset() && task.getStartOffset() == null)) {
			response(task, null, null);
		} else {
			m_scheduledThreadPool.submit(new Runnable() {
				@Override
				public void run() {
					executeTask(task, new ExponentialSchedulePolicy(//
					      m_config.getLongPollingCheckIntervalBaseMillis(),//
					      m_config.getLongPollingCheckIntervalMaxMillis()));
				}
			});
		}
	}

	private void executeTask(final PullMessageTask pullMessageTask, final SchedulePolicy policy) {
		if (m_stopped.get()) {
			return;
		}
		try {
			// skip expired task
			if (pullMessageTask.getExpireTime() < m_systemClockService.now()) {
				if (log.isDebugEnabled()) {
					log.debug("Client expired(correlationId={}, topic={}, partition={}, groupId={})", pullMessageTask
					      .getCorrelationId(), pullMessageTask.getTpg().getTopic(), pullMessageTask.getTpg().getPartition(),
					      pullMessageTask.getTpg().getGroupId());
				}
				return;
			}

			if (!pullMessageTask.getBrokerLease().isExpired()) {
				if (!queryAndResponseData(pullMessageTask)) {
					if (!m_stopped.get()) {
						m_scheduledThreadPool.schedule(new Runnable() {

							@Override
							public void run() {
								executeTask(pullMessageTask, policy);
							}
						}, policy.fail(false), TimeUnit.MILLISECONDS);
					}
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Broker no lease for this request(correlationId={}, topic={}, partition={}, groupId={})",
					      pullMessageTask.getCorrelationId(), pullMessageTask.getTpg().getTopic(), pullMessageTask.getTpg()
					            .getPartition(), pullMessageTask.getTpg().getGroupId());
				}
				// no lease, return empty cmd
				response(pullMessageTask, null, null);
			}
		} catch (Exception e) {
			log.error("Exception occurred while executing pull message task", e);
		}
	}

	private boolean queryAndResponseData(PullMessageTask pullTask) {
		Tpg tpg = pullTask.getTpg();

		MessageQueueCursor cursor = m_queueManager.getCursor(tpg, pullTask.getBrokerLease());

		if (cursor == null) {
			return false;
		}

		Pair<Offset, List<TppConsumerMessageBatch>> p = cursor.next(pullTask.getStartOffset(), pullTask.getBatchSize());

		Offset currentOffset = p.getKey();
		List<TppConsumerMessageBatch> batches = p.getValue();

		if (batches != null && !batches.isEmpty()) {

			String ip = NettyUtils.parseChannelRemoteAddr(pullTask.getChannel(), false);
			for (TppConsumerMessageBatch batch : batches) {
				m_queueManager.delivered(batch, tpg.getGroupId(), pullTask.isWithOffset());

				bizLogDelivered(ip, batch.getMessageMetas(), tpg);
			}

			response(pullTask, batches, currentOffset);
			return true;
		} else {
			return false;
		}
	}

	private void bizLogDelivered(String ip, List<MessageMeta> metas, Tpg tpg) {
		for (MessageMeta meta : metas) {
			BizEvent event = new BizEvent("Message.Delivered");
			event.addData("msgId", meta.getOriginId());
			event.addData("topic", tpg.getTopic());
			event.addData("consumerIp", ip);
			event.addData("groupId", tpg.getGroupId());

			m_bizLogger.log(event);
		}
	}

	@Override
	protected void doStop() {
		m_scheduledThreadPool.shutdown();
	}
}
