package com.ctrip.hermes.core.transport.endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ClientEndpointChannel extends EndpointChannel {

	void startVirtualChannel(VirtualChannelEventListener listener);

}
