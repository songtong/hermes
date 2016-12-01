package com.ctrip.hermes.collector.state.impl;

import java.util.Date;

import com.ctrip.hermes.collector.state.State;

public class TPProduceState extends State {
	public static final String ID_FORMAT = "%d-%d-%d";
	private long m_topicId;
	private String m_storageType;
	private String m_topicName;
	private int m_partition;
	private long m_offsetPriority;
	private Date m_lastPriorityCreationDate;
	private long m_offsetNonPriority;
	private Date m_lastNonPriorityCreationDate;
	
	public long getTopicId() {
		return m_topicId;
	}

	public void setTopicId(long topicId) {
		m_topicId = topicId;
	}

	public String getTopicName() {
		return m_topicName;
	}

	public void setTopicName(String topicName) {
		m_topicName = topicName;
	}
	
	public String getStorageType() {
		return m_storageType;
	}

	public void setStorageType(String storageType) {
		m_storageType = storageType;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}
	
	public int getPartitionId() {
		return m_partition;
	}

	public void setPartitionId(int partition) {
		m_partition = partition;
	}

	public long getOffsetPriority() {
		return m_offsetPriority;
	}

	public void setOffsetPriority(long offsetPriority) {
		m_offsetPriority = offsetPriority;
	}

	public Date getLastPriorityCreationDate() {
		return m_lastPriorityCreationDate;
	}

	public void setLastPriorityCreationDate(Date lastPriorityCreationDate) {
		m_lastPriorityCreationDate = lastPriorityCreationDate;
	}

	public long getOffsetNonPriority() {
		return m_offsetNonPriority;
	}

	public void setOffsetNonPriority(long offsetNonPriority) {
		m_offsetNonPriority = offsetNonPriority;
	}

	public Date getLastNonPriorityCreationDate() {
		return m_lastNonPriorityCreationDate;
	}

	public void setLastNonPriorityCreationDate(Date lastNonPriorityCreationDate) {
		m_lastNonPriorityCreationDate = lastNonPriorityCreationDate;
	}
	
	@Override
	protected void doUpdate(State state) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Object generateId() {
		return String.format(ID_FORMAT, this.m_topicId, this.m_partition, System.currentTimeMillis());
	}

}
