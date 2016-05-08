package com.ctrip.hermes.broker.longpolling;

import java.util.Date;
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
import com.ctrip.hermes.broker.status.BrokerStatusMonitor;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.FileBizLogger;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.DummyMessageMeta;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.dianping.cat.message.Transaction;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = LongPollingService.class)
public class DefaultLongPollingService extends AbstractLongPollingService implements Initializable {

	@Inject
	private FileBizLogger m_bizLogger;

	@Inject
	private MetaService m_metaService;

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
					      m_config.getLongPollingCheckIntervalBaseMillis(task.getTpg().getTopic()),//
					      m_config.getLongPollingCheckIntervalMaxMillis(task.getTpg().getTopic())));
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
		long startTime = System.currentTimeMillis();
		Tpg tpg = pullTask.getTpg();

		MessageQueueCursor cursor = m_queueManager.getCursor(tpg, pullTask.getBrokerLease(), pullTask.getStartOffset());

		if (cursor == null) {
			return false;
		}

		Pair<Offset, List<TppConsumerMessageBatch>> p = null;

		try {
			p = cursor.next(pullTask.getBatchSize(), pullTask.getFilter());
		} finally {
			cursor.stop();
		}

		if (p != null) {
			Offset currentOffset = p.getKey();
			List<TppConsumerMessageBatch> batches = p.getValue();

			if (batches != null && !batches.isEmpty()) {

				int count = 0;

				for (TppConsumerMessageBatch batch : batches) {
					// TODO remove legacy code
					boolean needServerSideAckHolder = pullTask.getPullMessageCommandVersion() < 3 ? true : false;
					m_queueManager.delivered(batch, tpg.getGroupId(), pullTask.isWithOffset(), needServerSideAckHolder);

					bizLogDelivered(pullTask.getClientIp(), batch.getMessageMetas(), tpg, pullTask.getReceiveTime());

					count += batch.size();
				}

				response(pullTask, batches, currentOffset);
				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_DELIVER_ELAPSE, tpg.getTopic(), startTime, count, null,
				      Transaction.SUCCESS);
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	private void bizLogDelivered(String ip, List<MessageMeta> metas, Tpg tpg, Date pullCmdReceiveTime) {
		BrokerStatusMonitor.INSTANCE.msgDelivered(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId(), ip, metas.size());

		for (MessageMeta meta : metas) {
			if (!(meta instanceof DummyMessageMeta)) {
				BizEvent event = new BizEvent("Message.Delivered");
				event.addData("msgId", meta.getOriginId());
				event.addData("topic", m_metaService.findTopicByName(tpg.getTopic()).getId());
				event.addData("partition", tpg.getPartition());
				event.addData("consumerIp", ip);
				event.addData("groupId", m_metaService.translateToIntGroupId(tpg.getTopic(), tpg.getGroupId()));
				event.addData("pullCmdReceiveTime", pullCmdReceiveTime);
				event.addData("isResend", meta.isResend());
				if (meta.isResend()) {
					event.addData("resendId", meta.getId());
				}

				m_bizLogger.log(event);
			}
		}
	}

	@Override
	protected void doStop() {
		m_scheduledThreadPool.shutdown();
	}
}
