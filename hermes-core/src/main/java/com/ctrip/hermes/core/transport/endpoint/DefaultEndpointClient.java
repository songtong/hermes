package com.ctrip.hermes.core.transport.endpoint;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;

@Named(type = EndpointClient.class)
public class DefaultEndpointClient extends AbstractEndpointClient {
	@Inject
	private EndpointManager m_endpointManager;

	@Override
	protected boolean isEndpointValid(Endpoint endpoint) {
		return m_endpointManager.containsEndpoint(endpoint);
	}
}
