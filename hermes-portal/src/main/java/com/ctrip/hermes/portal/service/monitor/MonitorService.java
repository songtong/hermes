package com.ctrip.hermes.portal.service.monitor;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.unidal.tuple.Pair;

public interface MonitorService {

	public Date getLatestProduced(String topic);

	public Pair<Date, Date> getDelay(String topic, int groupId);

	public Map<Integer, Pair<Date, Date>> getDelayDetails(String topic, int groupId);

	public Map<String, Set<String>> getTopic2ProducerIPs();

	public Map<String, Map<String, Set<String>>> getTopic2ConsumerIPs();

	public Map<String, Map<String, Set<String>>> getConsumerIP2Topics();

	public Map<String, Set<String>> getProducerIP2Topics();

	public List<String> getLatestBrokers();

	public List<String> getRelatedClients(String part);
}
