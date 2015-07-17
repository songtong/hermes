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
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventEngineContext;
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

	public Map<String, Map<Integer, Endpoint>> makeEndpoints(EventEngineContext context,
	      Map<String, Assignment<Integer>> brokerAssignments) throws Exception {

		Map<String, Map<Integer, Endpoint>> topicPartition2Endpoints = new HashMap<>();

		for (Map.Entry<String, Assignment<Integer>> topicAssignment : brokerAssignments.entrySet()) {

			String topic = topicAssignment.getKey();
			Map<Integer, Map<String, ClientContext>> assignment = topicAssignment.getValue().getAssignment();

			if (assignment != null && !assignment.isEmpty()) {

				topicPartition2Endpoints.put(topic, new HashMap<Integer, Endpoint>());

				for (Map.Entry<Integer, Map<String, ClientContext>> partitionAssignment : assignment.entrySet()) {
					topicPartition2Endpoints.get(topic).putAll(makePartition2Endpoints(context, topic, partitionAssignment));

				}

			}

		}

		return topicPartition2Endpoints;
	}

	private Map<Integer, Endpoint> makePartition2Endpoints(EventEngineContext context, String topic,
	      Map.Entry<Integer, Map<String, ClientContext>> partitionAssignment) throws Exception {

		Map<Integer, Endpoint> partition2Endpoints = new HashMap<>();

		int partition = partitionAssignment.getKey();
		Map<String, ClientContext> brokers = partitionAssignment.getValue();

		if (brokers != null && !brokers.isEmpty()) {
			Endpoint endpoint = new Endpoint();
			endpoint.setType(Endpoint.BROKER);

			Map<String, ClientLeaseInfo> brokerLease = m_brokerLeaseHolder.getAllValidLeases().get(
			      new Pair<String, Integer>(topic, partition));

			if (brokerLease == null || brokerLease.isEmpty()) {
				ClientContext broker = brokers.entrySet().iterator().next().getValue();

				endpoint.setHost(broker.getIp());
				endpoint.setId(broker.getName());
				endpoint.setPort(broker.getPort());
			} else {
				Entry<String, ClientLeaseInfo> brokerLeaseEntry = brokerLease.entrySet().iterator().next();
				ClientLeaseInfo broker = brokerLeaseEntry.getValue();
				Lease lease = broker.getLease();

				endpoint.setHost(broker.getIp());
				endpoint.setId(brokerLeaseEntry.getKey());
				endpoint.setPort(broker.getPort());

				scheduleLeaseExpireBrokerReblanceTask(context, lease);
			}

			partition2Endpoints.put(partition, endpoint);
		}

		return partition2Endpoints;
	}

	private void scheduleLeaseExpireBrokerReblanceTask(final EventEngineContext context, Lease lease) {
		long delayMillis = lease.getRemainingTime() > 0 ? lease.getRemainingTime()
		      + m_config.getleaseExpireRebalanceTriggerDelayMillis() : m_config
		      .getleaseExpireRebalanceTriggerDelayMillis();
		m_scheduledExecutor.schedule(new Runnable() {

			@Override
			public void run() {
				context.getEventBus().pubEvent(context, new Event(EventType.BROKER_LEASE_CHANGED, null));
			}
		}, delayMillis, TimeUnit.MILLISECONDS);
	}

	@Override
	public void initialize() throws InitializationException {
		m_scheduledExecutor = Executors.newScheduledThreadPool(m_config.getLeaseExpireRebalanceTriggerThreadCount(),
		      HermesThreadFactory.create("LeaseExpiredRebalanceTrigger", true));
	}

}
