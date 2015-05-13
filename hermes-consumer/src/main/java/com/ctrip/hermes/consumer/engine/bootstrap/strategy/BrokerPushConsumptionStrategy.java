package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.lease.LeaseManager.LeaseAcquisitionListener;
import com.ctrip.hermes.core.transport.command.SubscribeCommand;
import com.ctrip.hermes.core.transport.command.UnsubscribeCommand;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.transport.endpoint.VirtualChannelEventListener;
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
	public SubscribeHandle start(ConsumerContext context, int partitionId) {
		m_leaseManager.registerAcquisition(new Tpg(context.getTopic().getName(), partitionId, context.getGroupId()),
		      context.getSessionId(), new ConsumerAutoReconnectListener(context, partitionId));

		// TODO
		return null;

	}

	private class ConsumerAutoReconnectListener implements LeaseAcquisitionListener, VirtualChannelEventListener {
		private final ConsumerContext m_consumerContext;

		private final int m_partitionId;

		private AtomicReference<Long> m_correlationId = new AtomicReference<>(null);

		private AtomicReference<EndpointChannel> m_channel = new AtomicReference<EndpointChannel>();

		private ConsumerAutoReconnectListener(ConsumerContext consumerContext, int partitionId) {
			m_consumerContext = consumerContext;
			m_partitionId = partitionId;
		}

		private AtomicBoolean m_onAcquireCalled = new AtomicBoolean(false);

		private AtomicBoolean m_acquired = new AtomicBoolean(false);

		@Override
		public synchronized void onAcquire(Lease lease) {
			m_acquired.set(true);
			if (m_onAcquireCalled.compareAndSet(false, true)) {
				Endpoint endpoint = m_endpointManager.getEndpoint(m_consumerContext.getTopic().getName(), m_partitionId);
				m_clientEndpointChannelManager.startVirtualChannel(endpoint, this);
			} else {
				channelOpen(m_channel.get());
			}
		}

		@Override
		public synchronized void onExpire() {
			m_acquired.set(false);
			System.out.println("Lease expired..." + m_consumerContext);
			if (m_channel.get() != null) {
				UnsubscribeCommand cmd = new UnsubscribeCommand();
				cmd.getHeader().setCorrelationId(m_correlationId.get());
				m_channel.get().writeCommand(cmd);
			}

			deregisterConsumerNotifier();
			// TODO log
			// TODO if no tpg attach to this endpoint channel, close it
			// m_clientEndpointChannelManager.closeChannel(m_endpoint);
		}

		private void deregisterConsumerNotifier() {
			Long correlationId = m_correlationId.getAndSet(null);
			if (correlationId != null) {
				m_consumerNotifier.deregister(correlationId);
				// TODO
				System.out.println(String.format("Deregister consumer notifier(correlationId=%s)...", correlationId));
			}
		}

		@Override
		public synchronized void channelOpen(EndpointChannel channel) {
			if (m_acquired.get()) {
				m_channel.set(channel);

				SubscribeCommand subscribeCommand = new SubscribeCommand();
				subscribeCommand.setGroupId(m_consumerContext.getGroupId());
				subscribeCommand.setTopic(m_consumerContext.getTopic().getName());
				subscribeCommand.setPartition(m_partitionId);
				// TODO
				subscribeCommand.setWindow(10000);

				long correlationId = subscribeCommand.getHeader().getCorrelationId();
				if (m_correlationId.compareAndSet(null, correlationId)) {
					m_consumerNotifier.register(correlationId, m_consumerContext);
					m_channel.get().writeCommand(subscribeCommand);

					// TODO
					System.out.println("Subscribe command writed..." + subscribeCommand);
				}
			}
		}

		@Override
		public synchronized void channelClose() {
			deregisterConsumerNotifier();
		}

	}

}
