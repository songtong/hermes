package com.ctrip.hermes.portal.application;

public class ConsumerApplication extends HermesApplication {

	private String m_topicName;

	private String m_productLine;

	private String m_product;

	private String m_project;

	private String m_appName;

	private String m_onlineEnv;

	private String m_ownerName1;

	private String m_ownerName2;

	private String m_ownerPhone1;

	private String m_ownerPhone2;

	private int m_ackTimeoutSeconds;

	private boolean m_needRetry;

	private int m_retryCount;

	private int m_retryInterval;

	private boolean m_enabled;

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

	public String getOnlineEnv() {
		return m_onlineEnv;
	}

	public void setOnlineEnv(String onlineEnv) {
		m_onlineEnv = onlineEnv;
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

	public String getOwnerName1() {
		return m_ownerName1;
	}

	public void setOwnerName1(String ownerName1) {
		this.m_ownerName1 = ownerName1;
	}

	public String getOwnerName2() {
		return m_ownerName2;
	}

	public void setOwnerName2(String ownerName2) {
		this.m_ownerName2 = ownerName2;
	}

	public String getOwnerPhone1() {
		return m_ownerPhone1;
	}

	public void setOwnerPhone1(String ownerPhone1) {
		this.m_ownerPhone1 = ownerPhone1;
	}

	public String getOwnerPhone2() {
		return m_ownerPhone2;
	}

	public void setOwnerPhone2(String ownerPhone2) {
		this.m_ownerPhone2 = ownerPhone2;
	}

	public boolean isEnabled() {
	   return m_enabled;
   }

	public void setEnabled(boolean enabled) {
	   this.m_enabled = enabled;
   }

	@Override
   public String toString() {
	   return "ConsumerApplication [m_topicName=" + m_topicName + ", m_productLine=" + m_productLine + ", m_product="
	         + m_product + ", m_project=" + m_project + ", m_appName=" + m_appName + ", m_onlineEnv=" + m_onlineEnv
	         + ", m_ownerName1=" + m_ownerName1 + ", m_ownerName2=" + m_ownerName2 + ", m_ownerPhone1=" + m_ownerPhone1
	         + ", m_ownerPhone2=" + m_ownerPhone2 + ", m_ackTimeoutSeconds=" + m_ackTimeoutSeconds + ", m_needRetry="
	         + m_needRetry + ", m_retryCount=" + m_retryCount + ", m_retryInterval=" + m_retryInterval + ", m_enabled="
	         + m_enabled + "]";
   }

	
}
