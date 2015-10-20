package com.ctrip.hermes.core.transport.endpoint;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Endpoint;

@Named(type = EndpointClient.class)
public class DefaultEndpointClient extends AbstractEndpointClient {
	@Inject
	private MetaService m_metaService;

	@Override
	protected boolean isEndpointValid(Endpoint endpoint) {
		return m_metaService.containsEndpoint(endpoint);
	}
}
