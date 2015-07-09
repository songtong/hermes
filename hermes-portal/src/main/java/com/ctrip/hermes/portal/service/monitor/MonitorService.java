package com.ctrip.hermes.portal.service.monitor;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

public interface MonitorService {

	public Date getLatestProduced(String topic);

	public Pair<Date, Date> getDelay(String topic, int groupId);

	public Map<Integer, Pair<Date, Date>> getDelayDetails(String topic, int groupId);

	public Map<String, List<String>> getTopic2ProducerIPs();

	public Map<String, Map<String, List<String>>> getTopic2ConsumerIPs();

	public Map<String, List<String>> getConsumerIP2Topics();

	public Map<String, List<String>> getProducerIP2Topics();
}
