package com.ctrip.hermes.metaserver.broker;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.lease.DefaultLease;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.commons.BaseAssignmentHolder;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder.LeaseOperationCallback;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerLeaseAllocator.class)
public class DefaultBrokerLeaseAllocator implements BrokerLeaseAllocator {

	private static final Logger log = LoggerFactory.getLogger(DefaultBrokerLeaseAllocator.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private ActiveBrokerListHolder m_activeBrokerList;

	@Inject
	private BrokerLeaseHolder m_leaseHolder;

	@Inject
	private BrokerAssignmentHolder m_assignmentHolder;

	private void heartbeat(String topic, String brokerName, String ip, int port) {
		m_activeBrokerList.heartbeat(topic, brokerName, ip, port);
	}

	@Override
	public LeaseAcquireResponse tryAcquireLease(String topic, int partition, String brokerName, String ip, int port)
	      throws Exception {

		heartbeat(topic, brokerName, ip, port);

		BaseAssignmentHolder<String, Integer>.Assignment topicAssignment = m_assignmentHolder.getAssignment(topic);
		if (topicAssignment == null) {
			return topicNoAssignment();
		} else {
			if (topicAssignment.isAssignTo(partition, brokerName)) {
				return acquireLease(topic, partition, brokerName);
			} else {
				return topicPartitionNotAssignToBroker(topic, partition);
			}
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(String topic, int partition, String brokerName, long leaseId, String ip,
	      int port) throws Exception {

		heartbeat(topic, brokerName, ip, port);

		BaseAssignmentHolder<String, Integer>.Assignment topicAssignment = m_assignmentHolder.getAssignment(topic);
		if (topicAssignment == null) {
			return topicNoAssignment();
		} else {
			if (topicAssignment.isAssignTo(partition, brokerName)) {
				return renewLease(topic, partition, brokerName, leaseId);
			} else {
				return topicPartitionNotAssignToBroker(topic, partition);
			}
		}
	}

	protected LeaseAcquireResponse topicNoAssignment() {
		return new LeaseAcquireResponse(false, null, m_systemClockService.now()
		      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
	}

	protected LeaseAcquireResponse topicPartitionNotAssignToBroker(String topic, int partition) throws Exception {
		return m_leaseHolder.executeLeaseOperation(new Pair<String, Integer>(topic, partition),
		      new LeaseOperationCallback() {

			      @Override
			      public LeaseAcquireResponse execute(Map<String, Lease> existingValidLeases) throws Exception {
				      if (existingValidLeases.isEmpty()) {
					      return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					            + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
				      } else {
					      Collection<Lease> leases = existingValidLeases.values();
					      // use the first lease's exp time
					      return new LeaseAcquireResponse(false, null, leases.iterator().next().getExpireTime());
				      }
			      }

		      });

	}

	protected LeaseAcquireResponse acquireLease(final String topic, final int partition, final String brokerName)
	      throws Exception {
		final Pair<String, Integer> contextKey = new Pair<String, Integer>(topic, partition);

		return m_leaseHolder.executeLeaseOperation(contextKey, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, Lease> existingValidLeases) throws Exception {

				if (existingValidLeases.isEmpty()) {
					Lease newLease = m_leaseHolder.newLease(contextKey, brokerName, existingValidLeases,
					      m_config.getBrokerLeaseTimeMillis());

					log.info("Acquire lease success(topic={}, partition={}, brokerName={}, leaseExpTime={}).", topic,
					      partition, brokerName, newLease.getExpireTime());

					return new LeaseAcquireResponse(true, new DefaultLease(newLease.getId(), newLease.getExpireTime()
					      + m_config.getBrokerLeaseClientSideAdjustmentTimeMills()), -1);
				} else {
					Lease existingLease = null;

					for (Map.Entry<String, Lease> entry : existingValidLeases.entrySet()) {
						Lease lease = entry.getValue();
						String leaseBrokerName = entry.getKey();
						if (leaseBrokerName.equals(brokerName)) {
							existingLease = lease;
							break;
						}
					}

					if (existingLease != null) {
						return new LeaseAcquireResponse(true, new DefaultLease(existingLease.getId(), existingLease
						      .getExpireTime() + m_config.getBrokerLeaseClientSideAdjustmentTimeMills()), -1);
					} else {
						Collection<Lease> leases = existingValidLeases.values();
						// use the first lease's exp time
						return new LeaseAcquireResponse(false, null, leases.iterator().next().getExpireTime());
					}
				}

			}

		});

	}

	protected LeaseAcquireResponse renewLease(final String topic, final int partition, final String brokerName,
	      final long leaseId) throws Exception {

		final Pair<String, Integer> contextKey = new Pair<String, Integer>(topic, partition);

		return m_leaseHolder.executeLeaseOperation(contextKey, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, Lease> existingValidLeases) throws Exception {
				if (existingValidLeases.isEmpty()) {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
				} else {
					Lease existingLease = null;

					for (Map.Entry<String, Lease> entry : existingValidLeases.entrySet()) {
						Lease lease = entry.getValue();
						String leaseBrokerName = entry.getKey();
						if (lease.getId() == leaseId && leaseBrokerName.equals(brokerName)) {
							existingLease = lease;
							break;
						}
					}

					if (existingLease != null) {
						m_leaseHolder.renewLease(contextKey, brokerName, existingValidLeases, existingLease,
						      m_config.getBrokerLeaseTimeMillis());
						log.info("Renew lease success(topic={}, partition={}, brokerName={}, leaseExpTime={}).", topic,
						      partition, brokerName, existingLease.getExpireTime());

						return new LeaseAcquireResponse(true, new DefaultLease(leaseId, existingLease.getExpireTime()
						      + m_config.getBrokerLeaseClientSideAdjustmentTimeMills()), -1L);
					} else {
						return new LeaseAcquireResponse(false, null, m_systemClockService.now()
						      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
					}
				}
			}

		});

	}

}
