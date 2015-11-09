package com.ctrip.hermes.portal.application;

import com.ctrip.hermes.portal.dal.application.Application;

public class TopicApplication extends HermesApplication {
	private String m_productLine;

	private String m_entity;

	private String m_event;

	private String m_storageType;

	private String m_codecType;

	private long m_maxMsgNumPerDay;

	private int m_retentionDays;

	private int m_size;

	private String m_ownerName;

	private String m_description;
	
	private String m_languageType;

	public String getProductLine() {
		return m_productLine;
	}

	public void setProductLine(String productLine) {
		this.m_productLine = productLine;
	}

	public String getEntity() {
		return m_entity;
	}

	public void setEntity(String entity) {
		this.m_entity = entity;
	}

	public String getEvent() {
		return m_event;
	}

	public void setEvent(String event) {
		this.m_event = event;
	}

	public String getStorageType() {
		return m_storageType;
	}

	public void setStorageType(String storageType) {
		this.m_storageType = storageType;
	}

	public String getCodecType() {
		return m_codecType;
	}

	public void setCodecType(String codecType) {
		this.m_codecType = codecType;
	}

	public long getMaxMsgNumPerDay() {
		return m_maxMsgNumPerDay;
	}

	public void setMaxMsgNumPerDay(long maxMsgNumPerDay) {
		this.m_maxMsgNumPerDay = maxMsgNumPerDay;
	}

	public int getRetentionDays() {
		return m_retentionDays;
	}

	public void setRetentionDays(int retentionDays) {
		this.m_retentionDays = retentionDays;
	}

	public int getSize() {
		return m_size;
	}

	public void setSize(int size) {
		this.m_size = size;
	}

	public String getDescription() {
		return m_description;
	}

	public void setDescription(String description) {
		this.m_description = description;
	}

	public String getOwnerName() {
		return m_ownerName;
	}

	public void setOwnerName(String ownerName) {
		this.m_ownerName = ownerName;
	}

	@Override
	public Application toDBEntity() {
		Application dbApp = new Application();
		dbApp.setId(this.getId());
		dbApp.setType(this.getType());
		dbApp.setStatus(this.getStatus());
		dbApp.setContent(this.getContent());
		dbApp.setComment(this.getComment());
		dbApp.setOwner(this.getOwnerEmail());
		dbApp.setApprover(this.getApprover());
		dbApp.setCreateTime(this.getCreateTime());
		return dbApp;
	}

	public String getLanguageType() {
		return m_languageType;
	}

	public void setLanguageType(String languageType) {
		this.m_languageType = languageType;
	}
	
}
