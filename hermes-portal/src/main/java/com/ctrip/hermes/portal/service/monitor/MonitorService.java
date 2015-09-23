package com.ctrip.hermes.portal.service.monitor;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.portal.resource.view.BrokerQPSBriefView;
import com.ctrip.hermes.portal.resource.view.BrokerQPSDetailView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView.DelayDetail;

public interface MonitorService {

	public Date getLatestProduced(String topic);

	public Long getDelay(String topic, String groupId);

	public Long getDelay(String topic);

	public List<TopicDelayDetailView> getTopDelays(int top);

	public TopicDelayDetailView getTopicDelayDetail(String topic);
	
	public List<DelayDetail> getDelayDetailForConsumer(String tipic, String consumer);

	public List<Pair<String, Date>> getTopOutdateTopic(int top);

	public Map<String, Set<String>> getTopic2ProducerIPs();

	public Map<String, Map<String, Set<String>>> getTopic2ConsumerIPs();

	public Map<String, Map<String, Set<String>>> getConsumerIP2Topics();

	public Map<String, Set<String>> getProducerIP2Topics();

	public List<String> getLatestBrokers();

	public List<String> getRelatedClients(String part);

	public List<BrokerQPSBriefView> getBrokerReceivedQPS();

	public List<BrokerQPSBriefView> getBrokerDeliveredQPS();

	public BrokerQPSDetailView getBrokerReceivedDetailQPS(String brokerIp);

	public BrokerQPSDetailView getBrokerDeliveredDetailQPS(String brokerIp);

}
