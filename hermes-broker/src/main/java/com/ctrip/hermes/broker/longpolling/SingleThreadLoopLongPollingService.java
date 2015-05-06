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
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.queue.partition.MessageQueuePartitionCursor;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
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

	private ConcurrentMap<Tpg, MessageQueuePartitionCursor> m_cursors = new ConcurrentHashMap<>();

	@Override
	public void schedulePush(Tpg tpg, long correlationId, int batchSize, EndpointChannel channel, long expireTime) {
		m_tasks.offer(new PullTask(tpg, correlationId, batchSize, channel, expireTime));
	}

	@Override
	public void initialize() throws InitializationException {
		// TODO
		m_eventLoopThread = new Thread(new Runnable() {

			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					try {
						PullTask pullTask = m_tasks.poll(20, TimeUnit.MILLISECONDS);

						if (pullTask == null) {
							continue;
						}

						if (pullTask.getExpireTime() < System.currentTimeMillis()) {
							continue;
						}

						Tpg tpg = pullTask.getTpg();

						if (!m_cursors.containsKey(tpg)) {
							MessageQueuePartitionCursor cursor = m_queueManager.createCursor(tpg);
							// TODO when to remove this cursor
							m_cursors.put(tpg, cursor);
						}

						List<TppConsumerMessageBatch> batches = m_cursors.get(tpg).next(pullTask.getBatchSize());

						if (batches != null && !batches.isEmpty()) {
							PullMessageAckCommand cmd = new PullMessageAckCommand();
							cmd.addBatches(batches);
							cmd.getHeader().setCorrelationId(pullTask.getCorrelationId());

							pullTask.getChannel().writeCommand(cmd);

							// notify ack manager
							for (TppConsumerMessageBatch batch : batches) {
								m_ackManager.delivered(new Tpp(batch.getTopic(), batch.getPartition(), batch.isPriority()),
								      tpg.getGroupId(), batch.isResend(), batch.getMsgSeqs());
							}
						} else {
							m_tasks.offer(pullTask);
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

	private static class PullTask {
		private Tpg m_tpg;

		private long m_correlationId;

		private int m_batchSize;

		private EndpointChannel m_channel;

		private long m_expireTime;

		public PullTask(Tpg tpg, long correlationId, int batchSize, EndpointChannel channel, long expireTime) {
			m_tpg = tpg;
			m_correlationId = correlationId;
			m_batchSize = batchSize;
			m_channel = channel;
			m_expireTime = expireTime;
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

	}

}
