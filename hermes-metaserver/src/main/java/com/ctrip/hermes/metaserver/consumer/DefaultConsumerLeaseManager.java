package com.ctrip.hermes.metaserver.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.DefaultLease;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseManager.class)
public class DefaultConsumerLeaseManager implements ConsumerLeaseManager, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerLeaseManager.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private ActiveConsumerListHolder m_activeConsumerList;

	@Inject
	private PartitionConsumerAssigningStrategy m_partitionAssigningStrategy;

	private ScheduledExecutorService m_scheduledExecutorService;

	private AtomicReference<HashMap<Pair<String, String>, TopicAssignment>> m_assignments = new AtomicReference<>(
	      new HashMap<Pair<String, String>, TopicAssignment>());

	private AtomicLong m_leaseIdGenerator = new AtomicLong();

	private ConcurrentMap<Pair<String, String>, LeaseAssignment> m_existingLeases = new ConcurrentHashMap<>();

	private ReentrantLock m_existingLeasesLock = new ReentrantLock();

	@Override
	public LeaseAcquireResponse tryAcquireLease(Tpg tpg, String consumerName) {

		heartbeat(tpg, consumerName);

		Pair<String, String> key = new Pair<>(tpg.getTopic(), tpg.getGroupId());
		TopicAssignment topicAssignment = m_assignments.get().get(key);
		if (topicAssignment == null) {
			return topicConsumerGroupNoAssignment();
		} else {
			if (topicAssignment.isAssignTo(tpg.getPartition(), consumerName)) {
				return acquireLease(tpg, consumerName);
			} else {
				return topicPartitionNotAssignToConsumer(tpg);
			}
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(Tpg tpg, String consumerName, long leaseId) {

		heartbeat(tpg, consumerName);

		Pair<String, String> key = new Pair<>(tpg.getTopic(), tpg.getGroupId());
		TopicAssignment topicAssignment = m_assignments.get().get(key);
		if (topicAssignment == null) {
			return topicConsumerGroupNoAssignment();
		} else {
			if (topicAssignment.isAssignTo(tpg.getPartition(), consumerName)) {
				return renewLease(tpg, consumerName, leaseId);
			} else {
				return topicPartitionNotAssignToConsumer(tpg);
			}
		}
	}

	private void heartbeat(Tpg tpg, String consumerName) {
		m_activeConsumerList.heartbeat(tpg.getTopic(), tpg.getGroupId(), consumerName);
	}

	private LeaseAcquireResponse renewLease(Tpg tpg, String consumerName, long leaseId) {
		LeaseAssignment leaseAssignment = getLeaseAssignment(tpg.getTopic(), tpg.getGroupId());

		return leaseAssignment.renewLease(tpg.getPartition(), consumerName, leaseId);
	}

	private LeaseAcquireResponse topicPartitionNotAssignToConsumer(Tpg tpg) {
		LeaseAssignment leaseAssignment = getLeaseAssignment(tpg.getTopic(), tpg.getGroupId());

		Lease existingLease = leaseAssignment.getExistingLease(tpg.getPartition());
		if (existingLease == null || existingLease.isExpired()) {
			return new LeaseAcquireResponse(false, null, m_systemClockService.now()
			      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMills());
		} else {
			return new LeaseAcquireResponse(false, null, existingLease.getExpireTime());
		}
	}

	private LeaseAcquireResponse topicConsumerGroupNoAssignment() {
		return new LeaseAcquireResponse(false, null, m_systemClockService.now()
		      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMills());
	}

	private LeaseAcquireResponse acquireLease(Tpg tpg, String consumerName) {
		LeaseAssignment leaseAssignment = getLeaseAssignment(tpg.getTopic(), tpg.getGroupId());

		return leaseAssignment.acquireLease(tpg.getPartition(), consumerName);
	}

	private LeaseAssignment getLeaseAssignment(String topic, String consumerGroup) {
		Pair<String, String> topicConsumerGroup = new Pair<>(topic, consumerGroup);
		m_existingLeasesLock.lock();
		try {
			LeaseAssignment leaseAssignment;
			if (!m_existingLeases.containsKey(topicConsumerGroup)) {
				m_existingLeases.put(topicConsumerGroup, new LeaseAssignment(topicConsumerGroup.getKey(),
				      topicConsumerGroup.getValue()));
			}
			leaseAssignment = m_existingLeases.get(topicConsumerGroup);
			return leaseAssignment;
		} finally {
			m_existingLeasesLock.unlock();
		}
	}

	@Override
	public void initialize() throws InitializationException {
		m_scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(
		      "RebalanceChecker", true));

		m_scheduledExecutorService.scheduleWithFixedDelay(new TopicAssignmentTask(), 0,
		      m_config.getActiveConsumerCheckIntervalTimeMillis(), TimeUnit.MILLISECONDS);
	}

	private class TopicAssignmentTask implements Runnable {
		@Override
		public void run() {
			try {

				Map<Pair<String, String>, Set<String>> changes = m_activeConsumerList.scanChanges(
				      m_config.getConsumerHeartbeatTimeoutMillis(), TimeUnit.MILLISECONDS);

				if (changes != null && !changes.isEmpty()) {
					HashMap<Pair<String, String>, TopicAssignment> newAssignments = new HashMap<>(m_assignments.get());
					for (Map.Entry<Pair<String, String>, Set<String>> change : changes.entrySet()) {
						Pair<String, String> topicGroup = change.getKey();
						Set<String> consumerList = change.getValue();

						if (consumerList == null || consumerList.isEmpty()) {
							newAssignments.remove(topicGroup);
						} else {
							TopicAssignment newAssignment = createNewAssignment(topicGroup, consumerList);
							if (newAssignment != null) {
								newAssignments.put(topicGroup, newAssignment);
							}
						}
					}

					m_assignments.set(newAssignments);

					if (log.isDebugEnabled()) {
						StringBuilder sb = new StringBuilder();

						for (Map.Entry<Pair<String, String>, TopicAssignment> entry : newAssignments.entrySet()) {
							sb.append("[");
							sb.append("topic=").append(entry.getKey().getKey()).append(",");
							sb.append("consumerGroup=").append(entry.getKey().getValue()).append(",");
							sb.append("assignment=").append(entry.getValue());
							sb.append("]");
						}

						log.debug("Assignment changed.(new assignment={})", sb.toString());
					}
				}
			} catch (Exception e) {
				log.error("Error occured while checking active consumers", e);
			}
		}

		private TopicAssignment createNewAssignment(Pair<String, String> topicGroup, Set<String> consumerList) {
			Topic topic = m_metaHolder.getMeta().findTopic(topicGroup.getKey());
			if (topic != null) {
				List<Partition> partitions = topic.getPartitions();
				if (partitions == null || partitions.isEmpty()) {
					return null;
				}

				Map<String, List<Integer>> assigns = m_partitionAssigningStrategy.assign(partitions, consumerList);

				TopicAssignment assignment = new TopicAssignment();

				for (Map.Entry<String, List<Integer>> entry : assigns.entrySet()) {
					for (Integer partition : entry.getValue()) {
						assignment.addAssignment(partition, entry.getKey());
					}
				}

				return assignment;
			} else {
				return null;
			}
		}
	}

	private class TopicAssignment {
		private Map<Integer, String> m_partition2ConsumerInfos = new ConcurrentHashMap<>();

		public boolean isAssignTo(int partition, String consumerName) {
			String assignedConsumerName = m_partition2ConsumerInfos.get(partition);
			return assignedConsumerName != null && assignedConsumerName.equals(consumerName);
		}

		public void addAssignment(int partition, String consumerName) {
			m_partition2ConsumerInfos.put(partition, consumerName);
		}

		@Override
		public String toString() {
			return "TopicAssignment [m_partition2ConsumerInfos=" + m_partition2ConsumerInfos + "]";
		}

	}

	private class LeaseAssignment {
		private String m_topic;

		private String m_group;

		private Map<Integer, Pair<String, Lease>> m_leases = new HashMap<>();

		public LeaseAssignment(String topic, String group) {
			m_topic = topic;
			m_group = group;
		}

		public synchronized LeaseAcquireResponse renewLease(int partition, String consumerName, long leaseId) {
			Pair<String, Lease> existingLeasePair = m_leases.get(partition);
			if (existingLeasePair != null) {
				Lease existingLease = existingLeasePair.getValue();
				if (existingLease == null || existingLease.isExpired()) {
					m_leases.remove(consumerName);
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMills());
				} else {
					if (existingLease.getId() != leaseId) {
						return new LeaseAcquireResponse(false, null, existingLease.getExpireTime());
					} else {
						// extend expire time
						existingLease.setExpireTime(existingLease.getExpireTime() + m_config.getConsumerLeaseTimeMillis());
						log.info("Renew lease success(topic={}, consumerGroup={}, consumerName={}, leaseExpTime={}).",
						      m_topic, m_group, consumerName, existingLease.getExpireTime());

						return new LeaseAcquireResponse(true, new DefaultLease(leaseId, existingLease.getExpireTime()
						      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1L);
					}
				}
			} else {
				return new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMills());
			}
		}

		public synchronized LeaseAcquireResponse acquireLease(int partition, String consumerName) {
			Pair<String, Lease> existingLeasePair = m_leases.get(partition);
			if (existingLeasePair != null) {
				Lease existingLease = existingLeasePair.getValue();
				if (existingLease == null || existingLease.isExpired()) {
					return newLease(partition, consumerName);
				} else {
					return new LeaseAcquireResponse(false, null, existingLease.getExpireTime());
				}
			} else {
				return newLease(partition, consumerName);
			}
		}

		private LeaseAcquireResponse newLease(int partition, String consumerName) {
			long newLeaseId = m_leaseIdGenerator.incrementAndGet();

			DefaultLease lease = new DefaultLease(newLeaseId, m_systemClockService.now()
			      + m_config.getConsumerLeaseTimeMillis());
			m_leases.put(partition, new Pair<String, Lease>(consumerName, lease));

			log.info("Acquire lease success(topic={}, consumerGroup={}, consumerName={}, leaseExpTime={}).", m_topic,
			      m_group, consumerName, lease.getExpireTime());

			return new LeaseAcquireResponse(true, new DefaultLease(newLeaseId, lease.getExpireTime()
			      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1L);
		}

		public synchronized Lease getExistingLease(int partition) {
			Pair<String, Lease> existingLeasePair = m_leases.get(partition);
			if (existingLeasePair != null) {
				return existingLeasePair.getValue();
			}

			return null;
		}

	}

}