package com.ctrip.hermes.broker.longpolling;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.ack.AckManager;
import com.ctrip.hermes.broker.queue.MessageQueueCursor;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.transport.command.PullMessageAckCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = LongPollingService.class)
public class SingleThreadLoopLongPollingService implements LongPollingService, Initializable {
	@Inject
	private MessageQueueManager m_queueManager;

	@Inject
	private AckManager m_ackManager;

	private Thread m_eventLoopThread;

	// TODO size, if full?
	private LinkedBlockingQueue<PullTask> m_tasks = new LinkedBlockingQueue<>();

	private ConcurrentMap<Tpg, Long> m_nextScheduleTimes = new ConcurrentHashMap<>();

	@Override
	public void schedulePush(Tpg tpg, long correlationId, int batchSize, EndpointChannel channel, long expireTime,
	      Lease brokerLease) {
		m_tasks.offer(new PullTask(tpg, correlationId, batchSize, channel, expireTime, brokerLease));
	}

	@Override
	public void initialize() throws InitializationException {
		// TODO
		m_eventLoopThread = new Thread(new Runnable() {

			@Override
			public void run() {
				int times = 0;
				while (!Thread.currentThread().isInterrupted()) {
					try {
						long now = System.currentTimeMillis();
						PullTask pullTask = m_tasks.poll(50, TimeUnit.MILLISECONDS);

						if (pullTask == null) {
							continue;
						}

						// skip and remove expired task
						if (pullTask.getExpireTime() < System.currentTimeMillis()) {
							continue;
						}

						if (pullTask.getBrokerLease().getExpireTime() >= now) {
							if (!queryAndResponseData(now, pullTask)) {
								m_tasks.offer(pullTask);
							}
						} else {
							// no lease, return empty cmd
							response(pullTask, null);
						}
						// TODO
						if (times++ == 500) {
							times = 0;
							Thread.sleep(5);
						}
					} catch (InterruptedException e) {
						// TODO
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						// TODO
					}
				}
			}

		});

		m_eventLoopThread.setDaemon(true);
		m_eventLoopThread.setName("LongPollingThread");
		m_eventLoopThread.start();
	}

	private boolean queryAndResponseData(long now, PullTask pullTask) {
		Tpg tpg = pullTask.getTpg();

		m_nextScheduleTimes.putIfAbsent(tpg, now);

		MessageQueueCursor cursor = m_queueManager.getCursor(tpg, pullTask.getBrokerLease());
		long scheduleTime = m_nextScheduleTimes.get(tpg);

		List<TppConsumerMessageBatch> batches = null;

		if (scheduleTime <= now) {
			batches = cursor.next(pullTask.getBatchSize());
			// TODO query queue interval is 100 milliseconds, configable?
			m_nextScheduleTimes.put(tpg, now + 100L);
		} else {
			return false;
		}

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

	private void response(PullTask pullTask, List<TppConsumerMessageBatch> batches) {
		PullMessageAckCommand cmd = new PullMessageAckCommand();
		if (batches != null) {
			cmd.addBatches(batches);
		}
		cmd.getHeader().setCorrelationId(pullTask.getCorrelationId());

		pullTask.getChannel().writeCommand(cmd);
	}

	private static class PullTask {
		private Tpg m_tpg;

		private long m_correlationId;

		private int m_batchSize;

		private EndpointChannel m_channel;

		private long m_expireTime;

		private Lease m_brokerLease;

		public PullTask(Tpg tpg, long correlationId, int batchSize, EndpointChannel channel, long expireTime,
		      Lease brokerLease) {
			m_tpg = tpg;
			m_correlationId = correlationId;
			m_batchSize = batchSize;
			m_channel = channel;
			m_expireTime = expireTime;
			m_brokerLease = brokerLease;
		}

		public long getExpireTime() {
			return m_expireTime;
		}

		public Tpg getTpg() {
			return m_tpg;
		}

		public long getCorrelationId() {
			return m_correlationId;
		}

		public int getBatchSize() {
			return m_batchSize;
		}

		public EndpointChannel getChannel() {
			return m_channel;
		}

		public Lease getBrokerLease() {
			return m_brokerLease;
		}

	}

}
