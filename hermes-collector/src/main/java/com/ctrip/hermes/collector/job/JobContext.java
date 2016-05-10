package com.ctrip.hermes.collector.job;

import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.collector.record.Record;

public class JobContext {
	private String m_name;
	private JobGroup m_group;
	private boolean m_succeed;
	private boolean m_hasError;
	private Record<?> m_record; 
	private Map<String, Object> m_data = new HashMap<String, Object>();
	
	public JobContext() {}

	public String getName() {
		return m_name;
	}

	public void setName(String name) {
		m_name = name;
	}

	public JobGroup getGroup() {
		return m_group;
	}

	public void setGroup(JobGroup group) {
		m_group = group;
	}

	public boolean isSucceed() {
		return m_succeed;
	}

	public void setSucceed(boolean succeed) {
		m_succeed = succeed;
	}

	public boolean isHasError() {
		return m_hasError;
	}

	public void setHasError(boolean hasError) {
		m_hasError = hasError;
	}

	public Record<?> getRecord() {
		return m_record;
	}

	public void setRecord(Record<?> record) {
		m_record = record;
	}

	public Map<String, Object> getData() {
		return m_data;
	}

	public void setData(Map<String, Object> data) {
		m_data = data;
	}

	public boolean commitable() {
		return this.m_succeed && !this.m_hasError;
	}
}
