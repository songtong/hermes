package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.lease.LeaseManager.LeaseAcquisitionListener;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.SubscribeCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannelEventListener;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelActiveEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerBootstrap.class, value = Endpoint.BROKER)
public class BrokerConsumerBootstrap extends BaseConsumerBootstrap {

	@Inject
	private MetaService m_metaService;

	@Inject("consumer")
	private LeaseManager<Tpg> m_leaseManager;

	@Override
	protected void doStart(final ConsumerContext consumerContext) {

		List<Partition> partitions = m_metaService.getPartitions(consumerContext.getTopic().getName(),
		      consumerContext.getGroupId());

		for (final Partition partition : partitions) {

			m_leaseManager.registerAcquisition(
			      new Tpg(consumerContext.getTopic().getName(), partition.getId(), consumerContext.getGroupId()),
			      new LeaseAcquisitionListener() {

				      private Endpoint m_endpoint;

				      @Override
				      public void onClose() {
					      m_clientEndpointChannelManager.closeChannel(m_endpoint);
				      }

				      @Override
				      public void onAcquire() {
					      m_endpoint = m_endpointManager.getEndpoint(consumerContext.getTopic().getName(), partition.getId());
					      m_clientEndpointChannelManager.getChannel(m_endpoint, new ConsumerAutoReconnectListener(consumerContext,
					            partition));
				      }
			      });

		}

	}

	private class ConsumerAutoReconnectListener implements EndpointChannelEventListener {
		private final ConsumerContext m_consumerContext;

		private final Partition m_partition;

		private AtomicReference<Long> m_correlationId = new AtomicReference<>(null);

		private ConsumerAutoReconnectListener(ConsumerContext consumerContext, Partition partition) {
			m_consumerContext = consumerContext;
			m_partition = partition;
		}

		@Override
		public void onEvent(EndpointChannelEvent event) {
			if (event instanceof EndpointChannelActiveEvent) {
				channelActive(event.getChannel());
			} else if (event instanceof EndpointChannelInactiveEvent) {
				Long correlationId = m_correlationId.getAndSet(null);
				if (correlationId != null) {
					m_consumerNotifier.deregister(correlationId);
					// TODO
					System.out.println(String.format("Deregister consumer notifier(correlationId=%s)...", correlationId));
				}
			}
		}

		private void channelActive(EndpointChannel channel) {
			SubscribeCommand subscribeCommand = new SubscribeCommand();
			subscribeCommand.setGroupId(m_consumerContext.getGroupId());
			subscribeCommand.setTopic(m_consumerContext.getTopic().getName());
			subscribeCommand.setPartition(m_partition.getId());
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
}
