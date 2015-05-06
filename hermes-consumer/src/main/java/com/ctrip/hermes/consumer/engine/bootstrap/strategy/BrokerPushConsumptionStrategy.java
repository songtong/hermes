package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import java.util.concurrent.atomic.AtomicReference;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.lease.LeaseManager.LeaseAcquisitionListener;
import com.ctrip.hermes.core.transport.command.SubscribeCommand;
import com.ctrip.hermes.core.transport.command.UnsubscribeCommand;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannel;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannelEventListener;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelActiveEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BrokerPushConsumptionStrategy implements BrokerConsumptionStrategy {
	@Inject
	private LeaseManager<Tpg> m_leaseManager;

	@Inject
	private ConsumerNotifier m_consumerNotifier;

	@Inject
	private EndpointManager m_endpointManager;

	@Inject
	private ClientEndpointChannelManager m_clientEndpointChannelManager;

	@Override
	public void start(final ConsumerContext consumerContext, final int partitionId) {
		// TODO
		System.out.println(String.format("Start Push consumer bootstrap(topic=%s, partition=%s, consumerGroupId=%s)",
		      consumerContext.getTopic().getName(), partitionId, consumerContext.getGroupId()));

		m_leaseManager.registerAcquisition(
		      new Tpg(consumerContext.getTopic().getName(), partitionId, consumerContext.getGroupId()),
		      new LeaseAcquisitionListener() {

			      ConsumerAutoReconnectListener m_eventListener = new ConsumerAutoReconnectListener(consumerContext,
			            partitionId);

			      ClientEndpointChannel m_channel;

			      @Override
			      public void onExpire() {
				      System.out.println("Lease expired..." + consumerContext);
				      Long correlationId = m_eventListener.getCorrelationId();
				      if (correlationId != null) {
					      UnsubscribeCommand cmd = new UnsubscribeCommand();
					      cmd.getHeader().setCorrelationId(correlationId);
					      m_channel.writeCommand(cmd);
					      m_eventListener.onEvent(new UnsubscribeEvent());
					      // TODO log
					      // TODO if no tpg attach to this endpoint channel, close it
					      // m_clientEndpointChannelManager.closeChannel(m_endpoint);
				      }
			      }

			      @Override
			      public void onAcquire() {
				      Endpoint endpoint = m_endpointManager.getEndpoint(consumerContext.getTopic().getName(), partitionId);
				      m_channel = m_clientEndpointChannelManager.getChannel(endpoint, m_eventListener);
			      }
		      });

	}

	private class ConsumerAutoReconnectListener implements EndpointChannelEventListener {
		private final ConsumerContext m_consumerContext;

		private final int m_partitionId;

		private AtomicReference<Long> m_correlationId = new AtomicReference<>(null);

		private ConsumerAutoReconnectListener(ConsumerContext consumerContext, int partition) {
			m_consumerContext = consumerContext;
			m_partitionId = partition;
		}

		public Long getCorrelationId() {
			return m_correlationId.get();
		}

		@Override
		public void onEvent(EndpointChannelEvent event) {
			if (event instanceof EndpointChannelActiveEvent) {
				channelActive(event.getChannel());
			} else if (event instanceof EndpointChannelInactiveEvent) {
				deregisterConsumerNotifier();
			} else if (event instanceof UnsubscribeEvent) {
				deregisterConsumerNotifier();
			}
		}

		private void deregisterConsumerNotifier() {
			Long correlationId = m_correlationId.getAndSet(null);
			if (correlationId != null) {
				m_consumerNotifier.deregister(correlationId);
				// TODO
				System.out.println(String.format("Deregister consumer notifier(correlationId=%s)...", correlationId));
			}
		}

		private void channelActive(EndpointChannel channel) {
			SubscribeCommand subscribeCommand = new SubscribeCommand();
			subscribeCommand.setGroupId(m_consumerContext.getGroupId());
			subscribeCommand.setTopic(m_consumerContext.getTopic().getName());
			subscribeCommand.setPartition(m_partitionId);
			// TODO
			subscribeCommand.setWindow(10000);

			long correlationId = subscribeCommand.getHeader().getCorrelationId();
			if (m_correlationId.compareAndSet(null, correlationId)) {
				m_consumerNotifier.register(correlationId, m_consumerContext);
				channel.writeCommand(subscribeCommand);

				// TODO
				System.out.println("Subscribe command writed..." + subscribeCommand);
			}
		}
	}

	public class UnsubscribeEvent extends EndpointChannelEvent {

		public UnsubscribeEvent() {
			super(null, null);
		}

	}
}
