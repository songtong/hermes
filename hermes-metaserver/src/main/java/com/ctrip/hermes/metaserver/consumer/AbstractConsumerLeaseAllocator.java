package com.ctrip.hermes.metaserver.consumer;

import java.util.Collection;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.commons.BaseAssignmentHolder;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder.LeaseOperationCallback;
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

	protected void heartbeat(Tpg tpg, String consumerName, String ip, int port) {
		m_activeConsumerList
		      .heartbeat(new Pair<String, String>(tpg.getTopic(), tpg.getGroupId()), consumerName, ip, port);
	}

	@Override
	public LeaseAcquireResponse tryAcquireLease(Tpg tpg, String consumerName, String ip, int port) throws Exception {

		heartbeat(tpg, consumerName, ip, port);

		Pair<String, String> key = new Pair<>(tpg.getTopic(), tpg.getGroupId());
		BaseAssignmentHolder<Pair<String, String>, Integer>.Assignment topicAssignment = m_assignmentHolder
		      .getAssignment(key);
		if (topicAssignment == null) {
			return topicConsumerGroupNoAssignment();
		} else {
			if (topicAssignment.isAssignTo(tpg.getPartition(), consumerName)) {
				return acquireLease(tpg, consumerName, ip, port);
			} else {
				return topicPartitionNotAssignToConsumer(tpg);
			}
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(Tpg tpg, String consumerName, long leaseId, String ip, int port)
	      throws Exception {

		heartbeat(tpg, consumerName, ip, port);

		Pair<String, String> key = new Pair<>(tpg.getTopic(), tpg.getGroupId());
		BaseAssignmentHolder<Pair<String, String>, Integer>.Assignment topicAssignment = m_assignmentHolder
		      .getAssignment(key);
		if (topicAssignment == null) {
			return topicConsumerGroupNoAssignment();
		} else {
			if (topicAssignment.isAssignTo(tpg.getPartition(), consumerName)) {
				return renewLease(tpg, consumerName, leaseId, ip, port);
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

	protected LeaseAcquireResponse acquireLease(final Tpg tpg, final String consumerName, final String ip, final int port)
	      throws Exception {
		return m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
				return doAcquireLease(tpg, consumerName, existingValidLeases, ip, port);
			}

		});

	}

	protected LeaseAcquireResponse renewLease(final Tpg tpg, final String consumerName, final long leaseId,
	      final String ip, final int port) throws Exception {

		return m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
				return doRenewLease(tpg, consumerName, leaseId, existingValidLeases, ip, port);
			}

		});

	}

	protected abstract LeaseAcquireResponse doAcquireLease(final Tpg tpg, final String consumerName,
	      Map<String, ClientLeaseInfo> existingValidLeases, String ip, int port) throws Exception;

	protected abstract LeaseAcquireResponse doRenewLease(final Tpg tpg, final String consumerName, final long leaseId,
	      Map<String, ClientLeaseInfo> existingValidLeases, String ip, int port) throws Exception;

}
