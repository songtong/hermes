package com.ctrip.hermes.core.bo;

import com.ctrip.hermes.meta.entity.ConsumerGroup;

public class ConsumerView {
	private String topicName;

	

	private String groupName;

	private String appId;

	private String retryPolicy;

	private Integer ackTimeoutSeconds;

	private boolean orderedConsume;

	public ConsumerView() {
	}

	public ConsumerView(String topicName, ConsumerGroup consumer) {
		this.topicName = topicName;
		this.groupName = consumer.getName();
		this.appId = consumer.getAppIds();
		this.retryPolicy = consumer.getRetryPolicy();
		this.ackTimeoutSeconds = consumer.getAckTimeoutSeconds();
		this.orderedConsume = consumer.getOrderedConsume();
	}

	public  ConsumerGroup toMetaConsumer() {
		ConsumerGroup consumer = new ConsumerGroup();
		consumer.setAckTimeoutSeconds(this.ackTimeoutSeconds);
		consumer.setAppIds(this.appId);
		consumer.setName(this.groupName);
		consumer.setRetryPolicy(this.retryPolicy);
		consumer.setOrderedConsume(this.orderedConsume);

		return consumer;
	}


	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
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
		return "ConsumerView [topicName=" + topicName + ", groupName=" + groupName + ", appId=" + appId
				+ ", retryPolicy=" + retryPolicy + ", ackTimeoutSeconds=" + ackTimeoutSeconds + ", orderedConsume="
				+ orderedConsume + "]";
	}
}
