package com.ctrip.hermes.core.bo;

import com.ctrip.hermes.meta.entity.ConsumerGroup;

public class ConsumerView {
	private Integer id;
	
	private String topicName;

	private String owner1;

	private String owner2;

	private String phone1;

	private String phone2;

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
		this.owner1 = consumer.getOwner1();
		this.owner2 = consumer.getOwner2();
		this.phone1 = consumer.getPhone1();
		this.phone2 = consumer.getPhone2();
		this.id = consumer.getId();
	}

	public ConsumerGroup toMetaConsumer() {
		ConsumerGroup consumer = new ConsumerGroup();
		consumer.setAckTimeoutSeconds(this.ackTimeoutSeconds);
		consumer.setAppIds(this.appId);
		consumer.setName(this.groupName);
		consumer.setRetryPolicy(this.retryPolicy);
		consumer.setOrderedConsume(this.orderedConsume);
		consumer.setOwner1(this.owner1);
		consumer.setOwner2(this.owner2);
		consumer.setPhone1(this.phone1);
		consumer.setPhone2(this.phone2);
		consumer.setId(this.id);

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

	public String getOwner1() {
		return owner1;
	}

	public void setOwner1(String owner1) {
		this.owner1 = owner1;
	}

	public String getOwner2() {
		return owner2;
	}

	public void setOwner2(String owner2) {
		this.owner2 = owner2;
	}

	public String getPhone1() {
		return phone1;
	}

	public void setPhone1(String phone1) {
		this.phone1 = phone1;
	}

	public String getPhone2() {
		return phone2;
	}

	public void setPhone2(String phone2) {
		this.phone2 = phone2;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "ConsumerView [id=" + id + ", topicName=" + topicName + ", owner1=" + owner1 + ", owner2=" + owner2
				+ ", phone1=" + phone1 + ", phone2=" + phone2 + ", groupName=" + groupName + ", appId=" + appId
				+ ", retryPolicy=" + retryPolicy + ", ackTimeoutSeconds=" + ackTimeoutSeconds + ", orderedConsume="
				+ orderedConsume + "]";
	}


	

}
