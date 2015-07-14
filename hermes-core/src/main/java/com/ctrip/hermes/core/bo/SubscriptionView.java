package com.ctrip.hermes.core.bo;

import java.util.Objects;

public class SubscriptionView {

	private long id;

	private String name;

	private String topic;

	private String group;

	private String endpoints;

	private String status;

	public String getStatus() {
		return status;
	}

	public String getEndpoints() {
		return endpoints;
	}

	public String getGroup() {
		return group;
	}

	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getTopic() {
		return topic;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void setEndpoints(String endpoints) {
		this.endpoints = endpoints;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public void setId(long id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int hashCode() {
		return Objects.hash(this.id, this.name);
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (!(obj instanceof SubscriptionView)) {
			return false;
		}

		SubscriptionView other = (SubscriptionView) obj;
		return Objects.equals(this.id, other.id) && Objects.equals(this.name, other.name);
	}

	public String toString() {
		return new StringBuilder().append("SubscriptionView{").append("id=").append(this.id).append(",name=")
		      .append(name).append(",topic=").append(topic).append(",group=").append(group).append(",endpoints=")
		      .append(endpoints).append(",status=").append(status).append("}").toString();
	}
}
