package com.ctrip.hermes.metaserver.consumer;

import java.util.Collection;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.LeaseOperationCallback;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractConsumerLeaseAllocator implements ConsumerLeaseAllocator {

	@Inject
	protected MetaServerConfig m_config;

	@Inject
	protected SystemClockService m_systemClockService;

	@Inject
	protected ActiveConsumerListHolder m_activeConsumerList;

	@Inject
	protected ConsumerLeaseHolder m_leaseHolder;

	@Inject
	protected ConsumerAssignmentHolder m_assignmentHolder;

	public void setConfig(MetaServerConfig config) {
		m_config = config;
	}

	public void setSystemClockService(SystemClockService systemClockService) {
		m_systemClockService = systemClockService;
	}

	public void setActiveConsumerList(ActiveConsumerListHolder activeConsumerList) {
		m_activeConsumerList = activeConsumerList;
	}

	public void setLeaseHolder(ConsumerLeaseHolder leaseHolder) {
		m_leaseHolder = leaseHolder;
	}

	public void setAssignmentHolder(ConsumerAssignmentHolder assignmentHolder) {
		m_assignmentHolder = assignmentHolder;
	}

	protected void heartbeat(Tpg tpg, String consumerName, String ip) {
		m_activeConsumerList.heartbeat(new Pair<String, String>(tpg.getTopic(), tpg.getGroupId()), consumerName, ip);
	}

	@Override
	public LeaseAcquireResponse tryAcquireLease(Tpg tpg, String consumerName, String ip) throws Exception {

		heartbeat(tpg, consumerName, ip);

		Pair<String, String> topicGroup = new Pair<>(tpg.getTopic(), tpg.getGroupId());
		Assignment<Integer> topicGroupAssignment = m_assignmentHolder.getAssignment(topicGroup);
		if (topicGroupAssignment == null) {
			return topicConsumerGroupNoAssignment();
		} else {
			if (topicGroupAssignment.isAssignTo(tpg.getPartition(), consumerName)) {
				return acquireLease(tpg, consumerName, ip);
			} else {
				return topicPartitionNotAssignToConsumer(tpg);
			}
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(Tpg tpg, String consumerName, long leaseId, String ip) throws Exception {

		heartbeat(tpg, consumerName, ip);

		Pair<String, String> topicGroup = new Pair<>(tpg.getTopic(), tpg.getGroupId());
		Assignment<Integer> topicGroupAssignment = m_assignmentHolder.getAssignment(topicGroup);
		if (topicGroupAssignment == null) {
			return topicConsumerGroupNoAssignment();
		} else {
			if (topicGroupAssignment.isAssignTo(tpg.getPartition(), consumerName)) {
				return renewLease(tpg, consumerName, leaseId, ip);
			} else {
				return topicPartitionNotAssignToConsumer(tpg);
			}
		}
	}

	protected LeaseAcquireResponse topicConsumerGroupNoAssignment() {
		return new LeaseAcquireResponse(false, null, m_systemClockService.now()
		      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
	}

	protected LeaseAcquireResponse topicPartitionNotAssignToConsumer(Tpg tpg) throws Exception {
		return m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) {
				if (existingValidLeases.isEmpty()) {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
				} else {
					Collection<ClientLeaseInfo> leases = existingValidLeases.values();
					// use the first lease's exp time
					return new LeaseAcquireResponse(false, null, leases.iterator().next().getLease().getExpireTime());
				}
			}

		});

	}

	protected LeaseAcquireResponse acquireLease(final Tpg tpg, final String consumerName, final String ip)
	      throws Exception {
		return m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
				return doAcquireLease(tpg, consumerName, existingValidLeases, ip);
			}

		});

	}

	protected LeaseAcquireResponse renewLease(final Tpg tpg, final String consumerName, final long leaseId,
	      final String ip) throws Exception {

		return m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
				return doRenewLease(tpg, consumerName, leaseId, existingValidLeases, ip);
			}

		});

	}

	protected abstract LeaseAcquireResponse doAcquireLease(final Tpg tpg, final String consumerName,
	      Map<String, ClientLeaseInfo> existingValidLeases, String ip) throws Exception;

	protected abstract LeaseAcquireResponse doRenewLease(final Tpg tpg, final String consumerName, final long leaseId,
	      Map<String, ClientLeaseInfo> existingValidLeases, String ip) throws Exception;

}
