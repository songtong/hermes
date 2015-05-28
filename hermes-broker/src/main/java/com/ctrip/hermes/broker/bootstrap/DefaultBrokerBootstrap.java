package com.ctrip.hermes.broker.bootstrap;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.transport.NettyServer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerBootstrap.class)
public class DefaultBrokerBootstrap extends ContainerHolder implements BrokerBootstrap {

	private static final Logger log = LoggerFactory.getLogger(DefaultBrokerBootstrap.class);

	@Inject
	private NettyServer m_nettyServer;

	@Inject
	private BrokerConfig m_config;

	@Override
	public void start() throws Exception {
		// TODO should move to start script -D cause ByteBufUtil will read in static initialization
		System.setProperty("io.netty.allocator.type", "pooled");
		ChannelFuture future = m_nettyServer.start(m_config.getListeningPort());

		future.addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					log.info("Broker started at port {}.", m_config.getListeningPort());
				} else {
					log.error("Failed to start broker.");
				}

			}
		});

		future.channel().closeFuture().addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				log.info("Broker stopped.");
			}
		});
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
	}

}
