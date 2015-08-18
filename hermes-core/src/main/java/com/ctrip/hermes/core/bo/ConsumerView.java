package com.ctrip.hermes.core.bo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.meta.entity.ConsumerGroup;

public class ConsumerView {
	private List<String> topicNames;

	private String groupName;

	private String appId;

	private String retryPolicy;

	private Integer ackTimeoutSeconds;

	private boolean orderedConsume;

	public ConsumerView() {
	}

	public ConsumerView(List<String> topicNames, ConsumerGroup consumer) {
		this.topicNames = topicNames;
		this.groupName = consumer.getName();
		this.appId = consumer.getAppIds();
		this.retryPolicy = consumer.getRetryPolicy();
		this.ackTimeoutSeconds = consumer.getAckTimeoutSeconds();
		this.orderedConsume = consumer.getOrderedConsume();
	}

	public Map<String,ConsumerGroup> toMetaConsumer() {
		Map<String, ConsumerGroup> topicConsumerMap = new HashMap<>();
		for(String topicName : this.topicNames){
			ConsumerGroup consumer = new ConsumerGroup();
			consumer.setAckTimeoutSeconds(this.ackTimeoutSeconds);
			consumer.setAppIds(this.appId);
			consumer.setName(this.groupName);
			consumer.setRetryPolicy(this.retryPolicy);
			consumer.setOrderedConsume(this.orderedConsume);
			topicConsumerMap.put(topicName, consumer);
		}
		

		return topicConsumerMap;
	}

	public List<String> getTopicNames() {
		return topicNames;
	}

	
	
	public void setTopicNames(List<String> topicNames) {
		this.topicNames = topicNames;
	}
	
	

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getRetryPolicy() {
		return retryPolicy;
	}

	public void setRetryPolicy(String retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	public Integer getAckTimeoutSeconds() {
		return ackTimeoutSeconds;
	}

	public void setAckTimeoutSeconds(Integer ackTimeoutSeconds) {
		this.ackTimeoutSeconds = ackTimeoutSeconds;
	}

	public boolean isOrderedConsume() {
		return orderedConsume;
	}

	public void setOrderedConsume(boolean orderedConsume) {
		this.orderedConsume = orderedConsume;
	}

	
	@Override
	public String toString() {
		return "ConsumerView [topicNames=" + topicNames + ", groupName=" + groupName + ", appId=" + appId
				+ ", retryPolicy=" + retryPolicy + ", ackTimeoutSeconds=" + ackTimeoutSeconds + ", orderedConsume="
				+ orderedConsume + "]";
	}
}
