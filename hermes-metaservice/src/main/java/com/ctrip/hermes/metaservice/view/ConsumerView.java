package com.ctrip.hermes.metaservice.view;

public class ConsumerView {
	private Integer id;
	
	private String topicName;

	private String owner1;

	private String owner2;

	private String phone1;

	private String phone2;

	private String name;

	private String appIds;

	private String retryPolicy;

	private Integer ackTimeoutSeconds;

	private boolean orderedConsume;

	public ConsumerView() {
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAppIds() {
		return appIds;
	}

	public void setAppIds(String appIds) {
		this.appIds = appIds;
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
				+ ", phone1=" + phone1 + ", phone2=" + phone2 + ", name=" + name + ", appIds=" + appIds
				+ ", retryPolicy=" + retryPolicy + ", ackTimeoutSeconds=" + ackTimeoutSeconds + ", orderedConsume="
				+ orderedConsume + "]";
	}


	

}
