package com.ctrip.hermes.monitor.dashboard;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DashboardItem {

	private String m_category;

	private String m_topic;

	private int m_partition;

	private int m_priority;

	private int m_group;

	private Map<String, Object> m_value = new HashMap<String, Object>();

	private long m_samplingTimestamp;

	private Date m_timestamp;

	private String m_index;

	public String getIndex() {
		return m_index;
	}

	public void setIndex(String index) {
		m_index = index;
	}

	public Date getTimestamp() {
		return m_timestamp;
	}

	public void setTimestamp(Date timestamp) {
		m_timestamp = timestamp;
	}

	public long getSamplingTimestamp() {
		return m_samplingTimestamp;
	}

	public void setSamplingTimestamp(long samplingTimestamp) {
		m_samplingTimestamp = samplingTimestamp;
	}

	public void addValue(String key, Object value) {
		m_value.put(key, value);
	}

	public Map<String, Object> getValue() {
		return m_value;
	}

	public void setValue(Map<String, Object> value) {
		m_value = value;
	}

	public String getCategory() {
		return m_category;
	}

	public void setCategory(String category) {
		m_category = category;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public int getPriority() {
		return m_priority;
	}

	public void setPriority(int priority) {
		m_priority = priority;
	}

	public int getGroup() {
		return m_group;
	}

	public void setGroup(int group) {
		m_group = group;
	}

}
