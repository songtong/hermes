package com.ctrip.hermes.core.transport.endpoint;

public interface VirtualChannelEventListener {

	void channelOpen(EndpointChannel channel);
	
	void channelClose();

}
