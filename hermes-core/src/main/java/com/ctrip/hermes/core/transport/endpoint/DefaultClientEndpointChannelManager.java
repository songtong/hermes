package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelConnectFailedEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelExceptionCaughtEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClientEndpointChannelManager.class)
public class DefaultClientEndpointChannelManager implements ClientEndpointChannelManager {

	@Inject
	private CommandProcessorManager m_cmdProcessorManager;

	private ConcurrentMap<Endpoint, ClientEndpointChannel> m_channels = new ConcurrentHashMap<>();

	// TODO configable delay or with some strategy
	private int RECONNECT_DELAY_SECONDS = 1;

	@Override
	public ClientEndpointChannel getChannel(Endpoint endpoint, EndpointChannelEventListener... listeners) {
		switch (endpoint.getType()) {
		case Endpoint.BROKER:
			if (!m_channels.containsKey(endpoint)) {
				synchronized (m_channels) {
					if (!m_channels.containsKey(endpoint)) {
						ClientEndpointChannel channel = new NettyClientEndpointChannel(endpoint.getHost(),
						      endpoint.getPort(), m_cmdProcessorManager);

						channel.addListener(new NettyChannelAutoReconnectListener(RECONNECT_DELAY_SECONDS));
						channel.addListener(listeners);
						channel.start();
						m_channels.put(endpoint, channel);
					}
				}
			}

			return m_channels.get(endpoint);

		default:
			// TODO
			throw new IllegalArgumentException(String.format("unknow endpoint type: %s", endpoint.getType()));
		}
	}

	@Override
	public void closeChannel(Endpoint endpoint) {
		EndpointChannel removedChannel = null;

		switch (endpoint.getType()) {
		case Endpoint.BROKER:
			if (m_channels.containsKey(endpoint)) {
				synchronized (m_channels) {
					if (m_channels.containsKey(endpoint)) {
						removedChannel = m_channels.remove(endpoint);
					}
				}
			}
			break;

		default:
			// TODO
			throw new IllegalArgumentException(String.format("unknow endpoint type: %s", endpoint.getType()));
		}

		if (removedChannel != null) {
			removedChannel.close();
		}

	}

	protected static class NettyChannelAutoReconnectListener implements EndpointChannelEventListener {

		private int m_reconnectDelaySeconds;

		public NettyChannelAutoReconnectListener(int reconnectDelaySeconds) {
			m_reconnectDelaySeconds = reconnectDelaySeconds;
		}

		@Override
		public void onEvent(EndpointChannelEvent event) {
			EndpointChannel channel = event.getChannel();
			if (!channel.isClosed()) {
				if (event instanceof EndpointChannelConnectFailedEvent) {
					EventLoop eventLoop = event.getCtx();
					reconnect(eventLoop, channel);
				} else if (event instanceof EndpointChannelInactiveEvent) {
					ChannelHandlerContext ctx = event.getCtx();
					reconnect(ctx.channel().eventLoop(), channel);
				} else if (event instanceof EndpointChannelExceptionCaughtEvent) {
					ChannelHandlerContext ctx = event.getCtx();
					reconnect(ctx.channel().eventLoop(), channel);

					// TODO log exception
				}
			}
		}

		private void reconnect(EventLoop eventLoop, final EndpointChannel channel) {
			eventLoop.schedule(new Runnable() {

				@Override
				public void run() {
					// TODO log
					System.out.println("Reconnect...");
					channel.start();
				}
			}, m_reconnectDelaySeconds, TimeUnit.SECONDS);
		}

	}
}
