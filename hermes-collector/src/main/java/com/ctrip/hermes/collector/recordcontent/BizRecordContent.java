package com.ctrip.hermes.collector.recordcontent;

import org.codehaus.jackson.JsonNode;

import com.ctrip.hermes.collector.exception.SerializationException.DeserializeException;
import com.ctrip.hermes.collector.record.RecordContent;

public class BizRecordContent extends RecordContent {
	private String m_topicName;
	private int m_partition;
	private String m_consumerGroup;
	private String m_host;
	
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
	public String getConsumerGroup() {
		return m_consumerGroup;
	}
	public void setConsumerGroup(String consumerGroup) {
		m_consumerGroup = consumerGroup;
	}
	@Override
	public void deserialize(JsonNode json) throws DeserializeException {
		System.out.println(json);
	}
	
	public void bind(JsonNode json) {
		
	}
}
