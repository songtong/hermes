package com.ctrip.hermes.portal.application;

public class TopicApplication extends HermesApplication {
	private String m_productLine;

	private String m_entity;

	private String m_event;

	private String m_storageType;

	private String m_codecType;

	private long m_maxMsgNumPerDay;

	private int m_retentionDays;

	private int m_size;

	private String m_onlineEnv;

	private String m_ownerName1;

	private String m_ownerName2;

	private String m_ownerPhone1;

	private String m_ownerPhone2;

	private String m_description;

	private String m_languageType;

	private boolean m_needCompress;

	private String m_compressionType;

	private int m_compressionLevel;

	private String m_baseCodecType;

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

	public String getOnlineEnv() {
		return m_onlineEnv;
	}

	public void setOnlineEnv(String onlineEnv) {
		m_onlineEnv = onlineEnv;
	}

	public String getDescription() {
		return m_description;
	}

	public void setDescription(String description) {
		this.m_description = description;
	}

	public String getLanguageType() {
		return m_languageType;
	}

	public void setLanguageType(String languageType) {
		this.m_languageType = languageType;
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

	public String getBaseCodecType() {
		return m_baseCodecType;
	}

	public void setBaseCodecType(String baseCodecType) {
		this.m_baseCodecType = baseCodecType;
	}

	public int getCompressionLevel() {
		return m_compressionLevel;
	}

	public void setCompressionLevel(int compressionLevel) {
		this.m_compressionLevel = compressionLevel;
	}

	public boolean isNeedCompress() {
		return m_needCompress;
	}

	public void setNeedCompress(boolean needCompress) {
		this.m_needCompress = needCompress;
	}

	public String getCompressionType() {
		return m_compressionType;
	}

	public void setCompressionType(String compressionType) {
		this.m_compressionType = compressionType;
	}

}
