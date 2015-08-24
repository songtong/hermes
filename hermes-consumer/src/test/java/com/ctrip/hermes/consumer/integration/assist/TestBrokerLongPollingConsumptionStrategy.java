package com.ctrip.hermes.consumer.integration.assist;

import com.ctrip.hermes.consumer.engine.bootstrap.strategy.BrokerLongPollingConsumptionStrategy;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;

public class TestBrokerLongPollingConsumptionStrategy extends BrokerLongPollingConsumptionStrategy {
	public void setEndpointClient(EndpointClient endpointClient) {
		m_endpointClient = endpointClient;
	}
}
