package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventType;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EndpointMaker.class)
public class EndpointMaker implements Initializable {

	@Inject
	private BrokerLeaseHolder m_brokerLeaseHolder;

	@Inject
	private MetaServerConfig m_config;

	private ScheduledExecutorService m_scheduledExecutor;

	public void setBrokerLeaseHolder(BrokerLeaseHolder brokerLeaseHolder) {
		m_brokerLeaseHolder = brokerLeaseHolder;
	}

	public void setConfig(MetaServerConfig config) {
		m_config = config;
	}

	public void setScheduledExecutor(ScheduledExecutorService scheduledExecutor) {
		m_scheduledExecutor = scheduledExecutor;
	}

	public Map<String, Map<Integer, Endpoint>> makeEndpoints(EventBus eventBus, long version,
	      ClusterStateHolder stateHolder, Map<String, Assignment<Integer>> brokerAssignments) throws Exception {

		Map<String, Map<Integer, Endpoint>> topicPartition2Endpoints = new HashMap<>();

		for (Map.Entry<String, Assignment<Integer>> topicAssignment : brokerAssignments.entrySet()) {

			String topic = topicAssignment.getKey();
			Map<Integer, Map<String, ClientContext>> assignment = topicAssignment.getValue().getAssignments();

			if (assignment != null && !assignment.isEmpty()) {

				topicPartition2Endpoints.put(topic, new HashMap<Integer, Endpoint>());

				for (Map.Entry<Integer, Map<String, ClientContext>> partitionAssignment : assignment.entrySet()) {
					topicPartition2Endpoints.get(topic).putAll(
					      makePartition2Endpoints(eventBus, version, stateHolder, topic, partitionAssignment));

				}

			}

		}

		return topicPartition2Endpoints;
	}

	private Map<Integer, Endpoint> makePartition2Endpoints(EventBus eventBus, long version,
	      ClusterStateHolder stateHolder, String topic,
	      Map.Entry<Integer, Map<String, ClientContext>> partitionAssignment) throws Exception {

		Map<Integer, Endpoint> partition2Endpoints = new HashMap<>();

		int partition = partitionAssignment.getKey();
		Map<String, ClientContext> assignedBrokers = partitionAssignment.getValue();

		if (assignedBrokers != null && !assignedBrokers.isEmpty()) {
			Endpoint endpoint = new Endpoint();
			endpoint.setType(Endpoint.BROKER);

			Map<String, ClientLeaseInfo> brokerLease = m_brokerLeaseHolder.getAllValidLeases().get(
			      new Pair<String, Integer>(topic, partition));
			ClientContext assignedBroker = assignedBrokers.entrySet().iterator().next().getValue();

			if (brokerLease == null || brokerLease.isEmpty()) {

				endpoint.setHost(assignedBroker.getIp());
				endpoint.setId(assignedBroker.getName());
				endpoint.setPort(assignedBroker.getPort());
			} else {
				Entry<String, ClientLeaseInfo> brokerLeaseEntry = brokerLease.entrySet().iterator().next();
				String leaseHoldingBrokerName = brokerLeaseEntry.getKey();
				ClientLeaseInfo leaseHoldingBroker = brokerLeaseEntry.getValue();

				if (leaseHoldingBrokerName.equals(assignedBroker.getName())) {
					endpoint.setHost(assignedBroker.getIp());
					endpoint.setId(assignedBroker.getName());
					endpoint.setPort(assignedBroker.getPort());
				} else {
					Lease lease = leaseHoldingBroker.getLease();
					endpoint.setHost(leaseHoldingBroker.getIp());
					endpoint.setId(brokerLeaseEntry.getKey());
					endpoint.setPort(leaseHoldingBroker.getPort());

					scheduleLeaseExpireBrokerReblanceTask(eventBus, version, stateHolder, lease);
				}
			}

			partition2Endpoints.put(partition, endpoint);
		}

		return partition2Endpoints;
	}

	private void scheduleLeaseExpireBrokerReblanceTask(final EventBus eventBus, final long version,
	      final ClusterStateHolder stateHolder, Lease lease) {
		long delayMillis = lease.getRemainingTime() > 0 ? lease.getRemainingTime()
		      + m_config.getLeaseExpireRebalanceTriggerDelayMillis() : m_config
		      .getLeaseExpireRebalanceTriggerDelayMillis();
		m_scheduledExecutor.schedule(new Runnable() {

			@Override
			public void run() {
				eventBus.pubEvent(new Event(EventType.BROKER_LEASE_CHANGED, version, stateHolder, null));
			}
		}, delayMillis, TimeUnit.MILLISECONDS);
	}

	@Override
	public void initialize() throws InitializationException {
		m_scheduledExecutor = Executors.newScheduledThreadPool(m_config.getLeaseExpireRebalanceTriggerThreadCount(),
		      HermesThreadFactory.create("LeaseExpiredRebalanceTrigger", true));
	}

}
