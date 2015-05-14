package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseManager.ConsumerLeaseKey;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerLongPollingConsumptionStrategy implements BrokerConsumptionStrategy {
	@Inject
	private LeaseManager<ConsumerLeaseKey> m_leaseManager;

	@Inject
	private ConsumerNotifier m_consumerNotifier;

	@Inject
	private EndpointManager m_endpointManager;

	@Inject
	private ClientEndpointChannelManager m_clientEndpointChannelManager;

	@Inject
	private MessageCodec m_messageCodec;

	@Override
	public SubscribeHandle start(ConsumerContext context, int partitionId) {
		// TODO config
		int cacheSize = 50;
		long renewLeaseTimeMillisBeforeExpired = 2 * 1000L;
		long stopConsumerTimeMillsBeforLeaseExpired = 50L;

		LongPollingConsumerTask consumerTask = new LongPollingConsumerTask(context, partitionId, cacheSize,
		      renewLeaseTimeMillisBeforeExpired, stopConsumerTimeMillsBeforLeaseExpired);

		consumerTask.setClientEndpointChannelManager(m_clientEndpointChannelManager);
		consumerTask.setConsumerNotifier(m_consumerNotifier);
		consumerTask.setEndpointManager(m_endpointManager);
		consumerTask.setLeaseManager(m_leaseManager);
		consumerTask.setMessageCodec(m_messageCodec);

		Thread thread = new Thread(consumerTask, String.format("LongPollingExecutorThread-%s-%s-%s", context.getTopic()
		      .getName(), partitionId, context.getGroupId()));
		thread.start();
		// TODO
		return null;
	}
}
