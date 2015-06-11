package com.ctrip.hermes.core.bo;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.ConsumerGroup;

public class ConsumerView {
	private String topic;

	private String groupName;

	private String appId;

	private String retryPolicy;

	private Integer ackTimeoutSeconds;

	private boolean orderedConsume;

	public ConsumerView() {
	}

	public ConsumerView(String topic, ConsumerGroup consumer) {
		this.topic = topic;
		this.groupName = consumer.getName();
		this.appId = consumer.getAppIds();
		this.retryPolicy = consumer.getRetryPolicy();
		this.ackTimeoutSeconds = consumer.getAckTimeoutSeconds();
		this.orderedConsume = consumer.getOrderedConsume();
	}

	public Pair<String, ConsumerGroup> toMetaConsumer() {
		ConsumerGroup consumer = new ConsumerGroup();
		consumer.setAckTimeoutSeconds(this.ackTimeoutSeconds);
		consumer.setAppIds(this.appId);
		consumer.setName(this.groupName);
		consumer.setRetryPolicy(this.retryPolicy);
		consumer.setOrderedConsume(this.orderedConsume);

		return new Pair<String, ConsumerGroup>(this.topic, consumer);
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
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
		return "ConsumerView [topic=" + topic + ", groupName=" + groupName + ", appId=" + appId + ", retryPolicy="
		      + retryPolicy + ", ackTimeoutSeconds=" + ackTimeoutSeconds + ", orderedConsume=" + orderedConsume + "]";
	}
}
