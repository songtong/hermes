package com.ctrip.hermes.collector.recordcontent;

import org.codehaus.jackson.JsonNode;

import com.ctrip.hermes.collector.record.RecordContent;

/**
 * @author tenglinxiao
 * Describe the data for broker/metaserver services.
 */
public class ServiceRecordContent extends RecordContent{
	private Exception m_exception;
	
	public ServiceRecordContent(Exception exception) {
		this.m_exception = exception;
	}

	public Exception getException() {
		return m_exception;
	}

	public void setException(Exception exception) {
		this.m_exception = exception;
	}

	public void bind(JsonNode json) {
		
	}
}
