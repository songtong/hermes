package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.config.CoreConfig;
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

	private static final Logger log = LoggerFactory.getLogger(DefaultClientEndpointChannelManager.class);

	@Inject
	private CommandProcessorManager m_cmdProcessorManager;

	@Inject
	private CoreConfig m_config;

	private ConcurrentMap<Endpoint, ClientEndpointChannel> m_channels = new ConcurrentHashMap<>();

	@Override
	public ClientEndpointChannel getChannel(Endpoint endpoint) {
		switch (endpoint.getType()) {
		case Endpoint.BROKER:
			if (!m_channels.containsKey(endpoint)) {
				synchronized (m_channels) {
					if (!m_channels.containsKey(endpoint)) {
						ClientEndpointChannel channel = new NettyClientEndpointChannel(endpoint.getHost(),
						      endpoint.getPort(), m_cmdProcessorManager);

						channel.addListener(new NettyChannelAutoReconnectListener(m_config.getChannelAutoReconnectDelay()));
						channel.start();
						m_channels.put(endpoint, channel);
					}
				}
			}

			return m_channels.get(endpoint);

		default:
			throw new IllegalArgumentException(String.format("unknow endpoint type: %s", endpoint.getType()));
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
				}
			}
		}

		private void reconnect(EventLoop eventLoop, final EndpointChannel channel) {
			eventLoop.schedule(new Runnable() {

				@Override
				public void run() {
					// TODO host port
					log.info("Reconnect to {}:{}" + channel.getHost());
					channel.start();
				}
			}, m_reconnectDelaySeconds, TimeUnit.SECONDS);
		}

	}
}
