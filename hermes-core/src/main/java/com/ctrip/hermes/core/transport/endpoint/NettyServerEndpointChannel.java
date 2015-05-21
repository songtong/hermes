package com.ctrip.hermes.core.transport.endpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelExceptionCaughtEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;

public class NettyServerEndpointChannel extends NettyEndpointChannel {
	private static final Logger log = LoggerFactory.getLogger(NettyServerEndpointChannel.class);

	public NettyServerEndpointChannel(CommandProcessorManager cmdProcessorManager) {
		super(cmdProcessorManager);
		addListener(new EndpointChannelEventListener() {

			@Override
			public void onEvent(EndpointChannelEvent event) {
				if (event instanceof EndpointChannelInactiveEvent) {
					// TODO get channel ip and port
					log.warn("Channel inactive(host:{}, port:{})");
					close();
				} else if (event instanceof EndpointChannelExceptionCaughtEvent) {
					// TODO get channel ip and port
					log.warn("Channel exception caught(host:{}, port:{})");
					close();
				}
			}
		});
	}

	@Override
	public void start() {

	}

}
