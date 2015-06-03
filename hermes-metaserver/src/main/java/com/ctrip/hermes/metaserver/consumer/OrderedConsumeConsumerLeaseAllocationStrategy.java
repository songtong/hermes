package com.ctrip.hermes.metaserver.consumer;

import java.util.Collection;
import java.util.Map;

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
import com.ctrip.hermes.metaserver.build.BuildConstants;
import com.ctrip.hermes.metaserver.commons.BaseAssignmentHolder;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder.LeaseOperationCallback;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseAllocationStrategy.class, value = BuildConstants.LEASE_ALLOCATION_STRATEGY_ORDERED_CONSUME)
public class OrderedConsumeConsumerLeaseAllocationStrategy implements ConsumerLeaseAllocationStrategy {

	private static final Logger log = LoggerFactory.getLogger(OrderedConsumeConsumerLeaseAllocationStrategy.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private ActiveConsumerListHolder m_activeConsumerList;

	@Inject
	private ConsumerLeaseHolder m_leaseHolder;

	@Inject
	private ConsumerAssignmentHolder m_assignmentHolder;

	@Override
	public LeaseAcquireResponse tryAcquireLease(Tpg tpg, String consumerName) {

		heartbeat(tpg, consumerName);

		Pair<String, String> key = new Pair<>(tpg.getTopic(), tpg.getGroupId());
		BaseAssignmentHolder<Pair<String, String>, Integer>.Assignment topicAssignment = m_assignmentHolder
		      .getAssignment(key);
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
		BaseAssignmentHolder<Pair<String, String>, Integer>.Assignment topicAssignment = m_assignmentHolder
		      .getAssignment(key);
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
		m_activeConsumerList.heartbeat(new Pair<String, String>(tpg.getTopic(), tpg.getGroupId()), consumerName);
	}

	private LeaseAcquireResponse renewLease(final Tpg tpg, final String consumerName, final long leaseId) {
		return m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, Lease> existingValidLeases) {
				if (existingValidLeases.isEmpty()) {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMills());
				} else {
					Lease existingLease = null;

					for (Map.Entry<String, Lease> entry : existingValidLeases.entrySet()) {
						Lease lease = entry.getValue();
						String leaseConsumerName = entry.getKey();
						if (lease.getId() == leaseId && leaseConsumerName.equals(consumerName)) {
							existingLease = lease;
							break;
						}
					}

					if (existingLease != null) {
						existingLease.setExpireTime(existingLease.getExpireTime() + m_config.getConsumerLeaseTimeMillis());
						log.info("Renew lease success(topic={}, consumerGroup={}, consumerName={}, leaseExpTime={}).",
						      tpg.getTopic(), tpg.getGroupId(), consumerName, existingLease.getExpireTime());

						return new LeaseAcquireResponse(true, new DefaultLease(leaseId, existingLease.getExpireTime()
						      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1L);
					} else {
						return new LeaseAcquireResponse(false, null, m_systemClockService.now()
						      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMills());
					}
				}
			}

		});
	}

	private LeaseAcquireResponse topicPartitionNotAssignToConsumer(Tpg tpg) {
		return m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, Lease> existingValidLeases) {
				if (existingValidLeases.isEmpty()) {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMills());
				} else {
					Collection<Lease> leases = existingValidLeases.values();
					// use the first lease's exp time
					return new LeaseAcquireResponse(false, null, leases.iterator().next().getExpireTime());
				}
			}

		});

	}

	private LeaseAcquireResponse topicConsumerGroupNoAssignment() {
		return new LeaseAcquireResponse(false, null, m_systemClockService.now()
		      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMills());
	}

	private LeaseAcquireResponse acquireLease(final Tpg tpg, final String consumerName) {
		return m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, Lease> existingValidLeases) {
				if (existingValidLeases.isEmpty()) {
					Lease newLease = m_leaseHolder.newLease(m_config.getConsumerLeaseTimeMillis());
					existingValidLeases.put(consumerName, newLease);

					log.info("Acquire lease success(topic={}, consumerGroup={}, consumerName={}, leaseExpTime={}).",
					      tpg.getTopic(), tpg.getGroupId(), consumerName, newLease.getExpireTime());

					return new LeaseAcquireResponse(true, new DefaultLease(newLease.getId(), newLease.getExpireTime()
					      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1);
				} else {
					Collection<Lease> leases = existingValidLeases.values();
					// use the first lease's exp time
					return new LeaseAcquireResponse(false, null, leases.iterator().next().getExpireTime());
				}
			}

		});

	}

}