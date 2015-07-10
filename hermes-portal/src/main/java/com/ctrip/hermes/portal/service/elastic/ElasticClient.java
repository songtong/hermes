package com.ctrip.hermes.portal.service.elastic;

import java.util.List;
import java.util.Map;

public interface ElasticClient {

	public List<String> getLastWeekProducers(String topic);

	public List<String> getLastWeekConsumers(String topic, String consumer);

	public Map<String, Integer> getTopBrokerReceived();

	public Map<String, Integer> getTopBrokerDelivered();
}
