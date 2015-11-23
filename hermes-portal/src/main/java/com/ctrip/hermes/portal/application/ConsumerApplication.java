package com.ctrip.hermes.portal.application;

public class ConsumerApplication extends HermesApplication {

	private String m_topicName;

	private String m_productLine;

	private String m_product;

	private String m_project;

	private String m_appName;

	private String m_ownerName;

	private int m_ackTimeoutSeconds;

	private boolean m_needRetry;

	private int m_retryCount;

	private int m_retryInterval;

	public String getTopicName() {
		return m_topicName;
	}

	public void setTopicName(String topicName) {
		this.m_topicName = topicName;
	}

	public String getProductLine() {
		return m_productLine;
	}

	public void setProductLine(String productLine) {
		this.m_productLine = productLine;
	}

	public String getProject() {
		return m_project;
	}

	public void setProject(String project) {
		this.m_project = project;
	}

	public String getProduct() {
		return m_product;
	}

	public void setProduct(String product) {
		this.m_product = product;
	}

	public String getAppName() {
		return m_appName;
	}

	public void setAppName(String appName) {
		this.m_appName = appName;
	}

	public String getOwnerName() {
		return m_ownerName;
	}

	public void setOwnerName(String ownerName) {
		this.m_ownerName = ownerName;
	}

	public int getAckTimeoutSeconds() {
		return m_ackTimeoutSeconds;
	}

	public void setAckTimeoutSeconds(int ackTimeoutSeconds) {
		this.m_ackTimeoutSeconds = ackTimeoutSeconds;
	}

	public boolean isNeedRetry() {
		return m_needRetry;
	}

	public void setNeedRetry(boolean needRetry) {
		this.m_needRetry = needRetry;
	}

	public int getRetryCount() {
		return m_retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.m_retryCount = retryCount;
	}

	public int getRetryInterval() {
		return m_retryInterval;
	}

	public void setRetryInterval(int retryInterval) {
		this.m_retryInterval = retryInterval;
	}

}
