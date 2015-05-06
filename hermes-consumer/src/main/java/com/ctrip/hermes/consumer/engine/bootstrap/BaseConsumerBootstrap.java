package com.ctrip.hermes.consumer.engine.bootstrap;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.endpoint.ClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseConsumerBootstrap implements ConsumerBootstrap {

	@Inject
	protected ClientEndpointChannelManager m_clientEndpointChannelManager;

	@Inject
	protected EndpointManager m_endpointManager;

	@Inject
	protected MetaService m_metaService;

	@Inject
	protected ConsumerNotifier m_consumerNotifier;

	public void start(ConsumerContext context) {
		doStart(context);
	}

	public void stop(ConsumerContext context) {
		doStop(context);
	}

	protected void doStop(ConsumerContext context) {

	}

	protected abstract void doStart(ConsumerContext context);

}
