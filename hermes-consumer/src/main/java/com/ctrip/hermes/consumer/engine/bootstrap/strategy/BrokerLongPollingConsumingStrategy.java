package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseKey;
import com.ctrip.hermes.consumer.engine.monitor.PullMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerLongPollingConsumingStrategy implements BrokerConsumingStrategy {

	@Inject
	private LeaseManager<ConsumerLeaseKey> m_leaseManager;

	@Inject
	private ConsumerNotifier m_consumerNotifier;

	@Inject
	private EndpointManager m_endpointManager;

	@Inject
	protected EndpointClient m_endpointClient;

	@Inject
	private MessageCodec m_messageCodec;

	@Inject
	private ConsumerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private PullMessageResultMonitor m_pullMessageResultMonitor;

	@Inject
	private ClientEnvironment m_clientEnv;

	@Inject
	private MetaService m_metaService;

	@Override
	public SubscribeHandle start(ConsumerContext context, int partitionId) {

		try {
			// FIXME move to config
			int localCacheSize = Integer.valueOf(m_clientEnv.getConsumerConfig(context.getTopic().getName()).getProperty(
			      "consumer.localcache.size", m_config.getDefautlLocalCacheSize()));

			RetryPolicy retryPolicy = m_metaService.findRetryPolicyByTopicAndGroup(context.getTopic().getName(),
			      context.getGroupId());
			LongPollingConsumerTask consumerTask = new LongPollingConsumerTask(//
			      context, //
			      partitionId,//
			      localCacheSize, //
			      retryPolicy);

			consumerTask.setEndpointClient(m_endpointClient);
			consumerTask.setConsumerNotifier(m_consumerNotifier);
			consumerTask.setEndpointManager(m_endpointManager);
			consumerTask.setLeaseManager(m_leaseManager);
			consumerTask.setMessageCodec(m_messageCodec);
			consumerTask.setSystemClockService(m_systemClockService);
			consumerTask.setConfig(m_config);
			consumerTask.setPullMessageResultMonitor(m_pullMessageResultMonitor);

			Thread thread = HermesThreadFactory.create(
			      String.format("LongPollingExecutorThread-%s-%s-%s", context.getTopic().getName(), partitionId,
			            context.getGroupId()), false).newThread(consumerTask);
			thread.start();
			return new BrokerLongPollingSubscribeHandler(consumerTask);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Start Consumer failed(topic=%s, partition=%s, groupId=%s)", context
			      .getTopic().getName(), partitionId, context.getGroupId()), e);
		}
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
