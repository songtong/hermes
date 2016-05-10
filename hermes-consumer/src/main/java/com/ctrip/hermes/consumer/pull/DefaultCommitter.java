package com.ctrip.hermes.consumer.pull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.api.PullConsumerConfig;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.transport.command.v4.AckMessageCommandV4;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

public class DefaultCommitter<T> implements Committer<T> {

	private final static Logger log = LoggerFactory.getLogger(DefaultCommitter.class);

	private String m_topic;

	private String m_groupId;

	private PullConsumerConfig m_config;

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

	private BlockingQueue<RetriveSnapshot<T>> m_snapshots = new LinkedBlockingQueue<>();

	private AckManager m_ackManager;

	private ConsumerConfig m_consumerConfig;

	private List<ManualCommitTask> m_manualCommitTasks = new ArrayList<>();

	public DefaultCommitter(String topic, String groupId, int partitionCount, PullConsumerConfig config,
	      AckManager ackManager, ConsumerConfig consumerConfig) {
		m_topic = topic;
		m_groupId = groupId;
		m_config = config;
		m_ackManager = ackManager;
		m_consumerConfig = consumerConfig;

		int committerThreadCount = m_config.getCommitterThreadCount() <= 0 ? Math.min(10, partitionCount) : m_config
		      .getCommitterThreadCount();
		startCommiterThreads(topic, groupId, committerThreadCount);
	}

	private void startCommiterThreads(String topic, String groupId, int threadCount) {
		for (int i = 0; i < threadCount; i++) {
			ManualCommitTask manualCommitTask = new ManualCommitTask();
			m_manualCommitTasks.add(manualCommitTask);
			ThreadFactory factory = HermesThreadFactory.create("PullConsumerManualOffsetCommit-" + topic + "-" + groupId
			      + "-" + i, true);
			factory.newThread(manualCommitTask).start();
		}
	}

	private boolean writeAckToBroker(int partition, List<OffsetRecord> partitionRecords) {
		boolean success = true;

		if (partitionRecords != null && !partitionRecords.isEmpty()) {
			AckMessageCommandV4 cmd = new AckMessageCommandV4(m_topic, partition, m_groupId,
			      m_consumerConfig.getAckCheckerIoTimeoutMillis());

			for (OffsetRecord rec : partitionRecords) {
				if (rec.isNack()) {
					cmd.addNackMsg(rec.isPriority(), rec.isResend(), rec.getOffset(), rec.getRemainingRetries(), 0, 0);
				} else {
					cmd.addAckMsg(rec.isPriority(), rec.isResend(), rec.getOffset(), 0, 0, 0);
				}
			}
			success = m_ackManager.writeAckToBroker(cmd);
		}

		return success;
	}

	private class ManualCommitTask implements Runnable {
		private ExponentialSchedulePolicy m_retryPolicy;

		private ConcurrentMap<Integer, BlockingDeque<PartitionOperation<T>>> m_partitionOperations = new ConcurrentHashMap<>();

		public void submit(PartitionOperation<T> op) {
			int partition = op.getPartition();
			BlockingDeque<PartitionOperation<T>> opQueue = m_partitionOperations.get(partition);
			if (opQueue == null) {
				m_partitionOperations.putIfAbsent(partition, new LinkedBlockingDeque<PartitionOperation<T>>());
				opQueue = m_partitionOperations.get(partition);
			}

			opQueue.add(op);
		}

		@Override
		public void run() {
			int interval = m_config.getManualCommitInterval();
			m_retryPolicy = new ExponentialSchedulePolicy(interval, interval);

			while (!m_stopped.get()) {
				List<Entry<Integer, BlockingDeque<PartitionOperation<T>>>> entryList = new LinkedList<>(
				      m_partitionOperations.entrySet());
				Collections.shuffle(entryList);

				boolean ackWrote = false;
				for (Entry<Integer, BlockingDeque<PartitionOperation<T>>> entry : entryList) {
					int partition = entry.getKey();
					BlockingDeque<PartitionOperation<T>> opQueue = entry.getValue();
					PartitionOperation<T> op = null;
					try {

						op = opQueue.poll();
						if (op != null) {
							op = mergeMoreOperation(op, opQueue, 5000);

							if (writeAckToBroker(partition, op.getRecords())) {
								ackWrote = true;
								op.done();
								op = null;
							}
						}
					} catch (Exception e) {
						log.warn("Unexpected exception when commit consumer offet to broker, topic: {}, groupId: {}",
						      m_topic, m_groupId, e);
					} finally {
						if (op != null) {
							opQueue.addFirst(op);
						}
					}
				}

				if (!ackWrote) {
					m_retryPolicy.fail(true);
				} else {
					m_retryPolicy.succeess();
				}
			}
		}
	}

	private PartitionOperation<T> mergeMoreOperation(PartitionOperation<T> op,
	      BlockingDeque<PartitionOperation<T>> opQueue, int maxRecords) {

		while (!opQueue.isEmpty() && op.getRecords().size() < maxRecords) {
			if (op.getRecords().size() + opQueue.peek().getRecords().size() <= maxRecords) {
				PartitionOperation<T> moreOp = opQueue.poll();
				op.merge(moreOp);
			} else {
				break;
			}
		}

		return op;
	}

	private void commitSnapshotAsync(RetriveSnapshot<T> snapshot) {
		if (snapshot.getOffsetRecords().isEmpty()) {
			// ensure snapshot's Future is called
			snapshot.notifyFuture();
		} else {
			for (Entry<Integer, List<OffsetRecord>> entry : snapshot.getOffsetRecords().entrySet()) {
				Integer partition = entry.getKey();
				PartitionOperation<T> operation = new PartitionOperation<T>(partition, entry.getValue(), snapshot);
				m_manualCommitTasks.get(partition % m_manualCommitTasks.size()).submit(operation);
			}
		}
	}

	@Override
	public void close() {
		m_stopped.compareAndSet(false, true);
	}

	@Override
	public synchronized RetriveSnapshot<T> delivered(List<ConsumerMessage<T>> msgs) {
		RetriveSnapshot<T> snapshot = new RetriveSnapshot<T>(m_topic, msgs, this);
		m_snapshots.add(snapshot);
		return snapshot;
	}

	@Override
	public synchronized void scanAndCommitAsync() {
		while (!m_snapshots.isEmpty()) {
			RetriveSnapshot<T> snapshot = m_snapshots.peek();
			if (snapshot.isDone()) {
				m_snapshots.poll();
				commitSnapshotAsync(snapshot);
			} else {
				break;
			}
		}
	}

}
