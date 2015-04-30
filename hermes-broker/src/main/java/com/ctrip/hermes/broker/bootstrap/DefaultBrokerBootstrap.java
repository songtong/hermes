package com.ctrip.hermes.broker.bootstrap;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.transport.NettyServer;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerBootstrap.class)
public class DefaultBrokerBootstrap extends ContainerHolder implements BrokerBootstrap {

	@Inject
	private NettyServer m_nettyServer;

	@Override
	public void start() throws Exception {
		// TODO should move to start script -D cause ByteBufUtil will read in static initialization
		System.setProperty("io.netty.allocator.type", "pooled");
		m_nettyServer.start();
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
	}

}
