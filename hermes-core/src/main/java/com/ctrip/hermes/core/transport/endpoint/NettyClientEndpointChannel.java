package com.ctrip.hermes.core.transport.endpoint;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import com.ctrip.hermes.core.transport.codec.NettyDecoder;
import com.ctrip.hermes.core.transport.codec.NettyEncoder;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelActiveEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelConnectFailedEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelExceptionCaughtEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;

@Sharable
public class NettyClientEndpointChannel extends NettyEndpointChannel implements ClientEndpointChannel {

	private EventLoopGroup m_eventLoopGroup = new NioEventLoopGroup();

	private String m_host;

	private int m_port;

	public NettyClientEndpointChannel(String host, int port, CommandProcessorManager cmdProcessorManager) {
		super(cmdProcessorManager);
		m_host = host;
		m_port = port;
	}

	@Override
	public void start() {
		try {
			Bootstrap b = new Bootstrap();
			b.group(m_eventLoopGroup);
			b.channel(NioSocketChannel.class);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			b.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {

					ch.pipeline().addLast( //
					      // TODO set max frame length
					      new NettyDecoder(), //
					      new LengthFieldPrepender(4), //
					      new NettyEncoder(), //
					      NettyClientEndpointChannel.this);
				}
			});

			ChannelFuture future = b.connect(m_host, m_port);

			future.addListener(new ChannelFutureListener() {

				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					// TODO log
					if (!future.isSuccess()) {
						notifyListener(new EndpointChannelConnectFailedEvent(future.channel().eventLoop(),
						      NettyClientEndpointChannel.this));
					}
				}

			});

			future.sync();
		} catch (Exception e) {
			// TODO don't print log
		} finally {
		}
	}

	@Override
	public void startVirtualChannel(final VirtualChannelEventListener listener) {
		// TODO remove the listener
		addListener(new EndpointChannelEventListener() {

			@Override
			public void onEvent(EndpointChannelEvent event) {
				if (event instanceof EndpointChannelActiveEvent) {
					listener.channelOpen(NettyClientEndpointChannel.this);
				} else if (event instanceof EndpointChannelExceptionCaughtEvent) {
					listener.channelClose();
				} else if (event instanceof EndpointChannelInactiveEvent) {
					listener.channelClose();
				}
			}
		});

		listener.channelOpen(this);
	}

}
