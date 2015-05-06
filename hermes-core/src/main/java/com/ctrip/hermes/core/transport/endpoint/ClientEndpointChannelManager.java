package com.ctrip.hermes.core.transport.endpoint;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ClientEndpointChannelManager {

	ClientEndpointChannel getChannel(Endpoint endpoint);

	ClientEndpointChannel startVirtualChannel(Endpoint endpoint, VirtualChannelEventListener listener);

	void closeChannel(Endpoint endpoint);
}
