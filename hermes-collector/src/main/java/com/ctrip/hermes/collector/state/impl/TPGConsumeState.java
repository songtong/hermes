package com.ctrip.hermes.collector.state.impl;

import java.util.Date;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.ctrip.hermes.collector.state.State;

public class TPGConsumeState extends State {
	public static final String ID_FORMAT = "%d-%d-%d-%d";
	private String m_storageType;
	private long m_topicId;
	private String m_topicName;
	private int m_partition;
	private long m_consumerGroupId;
	private String m_consumerGroup;
	private long m_offsetPriority;
	private Date m_offsetPriorityModifiedDate;
	private long m_offsetNonPriority;
	private Date m_offsetNonPriorityModifiedDate;
	private long m_deadLetterCount; 
	
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

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}
	
	@JsonIgnore
	public int getPartitionId() {
		return m_partition;
	}

	public void setPartitionId(int partition) {
		m_partition = partition;
	}
	
	public String getStorageType() {
		return m_storageType;
	}

	public void setStorageType(String storageType) {
		m_storageType = storageType;
	}

	public long getConsumerGroupId() {
		return m_consumerGroupId;
	}

	public void setConsumerGroupId(long consumerGroupId) {
		m_consumerGroupId = consumerGroupId;
	}

	public String getConsumerGroup() {
		return m_consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		m_consumerGroup = consumerGroup;
	}

	public long getOffsetPriority() {
		return m_offsetPriority;
	}

	public void setOffsetPriority(long offsetPriority) {
		m_offsetPriority = offsetPriority;
	}

	public Date getOffsetPriorityModifiedDate() {
		return m_offsetPriorityModifiedDate;
	}

	public void setOffsetPriorityModifiedDate(Date offsetPriorityModifiedDate) {
		m_offsetPriorityModifiedDate = offsetPriorityModifiedDate;
	}

	public long getOffsetNonPriority() {
		return m_offsetNonPriority;
	}

	public void setOffsetNonPriority(long offsetNonPriority) {
		m_offsetNonPriority = offsetNonPriority;
	}

	public Date getOffsetNonPriorityModifiedDate() {
		return m_offsetNonPriorityModifiedDate;
	}

	public void setOffsetNonPriorityModifiedDate(Date offsetNonPriorityModifiedDate) {
		m_offsetNonPriorityModifiedDate = offsetNonPriorityModifiedDate;
	}
	
	public long getDeadLetterCount() {
		return m_deadLetterCount;
	}

	public void setDeadLetterCount(long deadLetterCount) {
		m_deadLetterCount = deadLetterCount;
	}

	@Override
	protected void doUpdate(State state) {		
	}

	@Override
	protected Object generateId() {
		return String.format(ID_FORMAT, this.m_topicId, this.m_partition, this.m_consumerGroupId, System.currentTimeMillis());
	}

}
