package com.ctrip.hermes.collector.recordcontent;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.springframework.http.HttpStatus;

import com.ctrip.hermes.collector.record.RecordContent;

public class StorageResultContent extends RecordContent {
	private HttpStatus m_status;
	private String m_errorMessage;
	private boolean m_hasError;
	private List<String> m_failedRecords;
	
	public StorageResultContent() {}
	
	public HttpStatus getStatus() {
		return m_status;
	}

	public void setStatus(HttpStatus status) {
		m_status = status;
	}

	public String getErrorMessage() {
		return m_errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.m_errorMessage = errorMessage;
	}

	public boolean hasError() {
		return m_hasError;
	}

	public void setHasError(boolean hasError) {
		m_hasError = hasError;
	}
	
	public List<String> getFailedRecords() {
		return m_failedRecords;
	}

	public void setFailedRecords(List<String> failedRecords) {
		m_failedRecords = failedRecords;
	}

	public void bind(JsonNode json) {
		if (json.has("status")) {
			m_status = HttpStatus.valueOf(json.get("status").asInt());
			m_errorMessage = json.get("error").asText();
		} else {
			m_hasError = json.get("errors").asBoolean();
			if (m_hasError) {
				m_failedRecords = new ArrayList<String>();
				ArrayNode items = (ArrayNode)json.get("items");
				for (JsonNode item : items) {
					if (item.get("status").asInt() != 201) {
						m_failedRecords.add(item.get("_id").asText());
					}
				}
			}
		}
	}
}
