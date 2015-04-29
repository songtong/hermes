package com.ctrip.hermes.core.transport.endpoint;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelExceptionCaughtEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;

public class NettyServerEndpointChannel extends NettyEndpointChannel {

	public NettyServerEndpointChannel(CommandProcessorManager cmdProcessorManager) {
		super(cmdProcessorManager);
		addListener(new EndpointChannelEventListener() {

			@Override
			public void onEvent(EndpointChannelEvent event) {
				if (event instanceof EndpointChannelInactiveEvent || event instanceof EndpointChannelExceptionCaughtEvent) {
					// TODO log
					close();
				}
			}
		});
	}

	@Override
	public void start() {

	}

}
