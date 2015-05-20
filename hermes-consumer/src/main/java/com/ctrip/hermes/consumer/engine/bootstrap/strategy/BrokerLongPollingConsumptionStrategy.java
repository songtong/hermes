package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseManager.ConsumerLeaseKey;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

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

	@Inject
	private ConsumerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Override
	public SubscribeHandle start(ConsumerContext context, int partitionId) {

		LongPollingConsumerTask consumerTask = new LongPollingConsumerTask(//
		      context, //
		      partitionId,//
		      m_config.getLocalCacheSize(), //
		      m_systemClockService);

		consumerTask.setClientEndpointChannelManager(m_clientEndpointChannelManager);
		consumerTask.setConsumerNotifier(m_consumerNotifier);
		consumerTask.setEndpointManager(m_endpointManager);
		consumerTask.setLeaseManager(m_leaseManager);
		consumerTask.setMessageCodec(m_messageCodec);
		consumerTask.setSystemClockService(m_systemClockService);
		consumerTask.setConfig(m_config);

		Thread thread = HermesThreadFactory.create(
		      String.format("LongPollingExecutorThread-%s-%s-%s", context.getTopic().getName(), partitionId,
		            context.getGroupId()), false).newThread(consumerTask);
		thread.start();
		return new BrokerLongPollingSubscribeHandler(consumerTask);
	}

	private static class BrokerLongPollingSubscribeHandler implements SubscribeHandle {

		private LongPollingConsumerTask m_consumerTask;

		public BrokerLongPollingSubscribeHandler(LongPollingConsumerTask consumerTask) {
			m_consumerTask = consumerTask;
		}

		@Override
		public void close() {
			m_consumerTask.close();
		}

	}
}
