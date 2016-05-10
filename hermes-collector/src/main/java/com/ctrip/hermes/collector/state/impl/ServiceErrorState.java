package com.ctrip.hermes.collector.state.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.ctrip.hermes.collector.state.State;

public class ServiceErrorState extends State {
	public static final String ID_FORMAT = "%s-%d";
	
	private SourceType m_sourceType;
	
	private Map<String, Long> m_countOnHosts;

	private long m_count;

	private Map<String, List<ErrorSample>> m_errorsOnHosts = new HashMap<>();
	
	public static ServiceErrorState newServiceErrorState(SourceType sourceType) {
		return new ServiceErrorState(sourceType);
	}
	
	private ServiceErrorState(SourceType sourceType) {
		this.m_sourceType = sourceType;
	}

	@JsonProperty("source")
	public SourceType getSourceType() {
		return m_sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.m_sourceType = sourceType;
	}
	
	public Map<String, Long> getCountOnHosts() {
		return m_countOnHosts;
	}

	@JsonIgnore
	public void setCountOnHosts(Map<String, Long> countOnHosts) {
		m_countOnHosts = countOnHosts;
	}

	public Map<String, List<ErrorSample>> getErrorsOnHosts() {
		return m_errorsOnHosts;
	}

	public void setErrorsOnHosts(Map<String, List<ErrorSample>> errorsOnHosts) {
		m_errorsOnHosts = errorsOnHosts;
	}

	public void addErrorOnHost(String host, ErrorSample errorSample) {
		if (!m_errorsOnHosts.containsKey(host)) {
			m_errorsOnHosts.put(host, new ArrayList<ErrorSample>());
		}
		
		m_errorsOnHosts.get(host).add(errorSample);
	}

	public long getCount() {
		return m_count;
	}

	public void setCount(long count) {
		m_count = count;
	}

	@Override
	protected void doUpdate(State state) {

	}
	
	public Object generateId() {
		return String.format(ID_FORMAT, this.m_sourceType.getName(), System.currentTimeMillis());
	}

	public static class ErrorSample {
		private long m_timestamp;
		private String m_hostname;
		private String m_message;

		public long getTimestamp() {
			return m_timestamp;
		}

		public void setTimestamp(long timestamp) {
			m_timestamp = timestamp;
		}

		public String getHostname() {
			return m_hostname;
		}

		public void setHostname(String hostname) {
			m_hostname = hostname;
		}

		public String getMessage() {
			return m_message;
		}

		public void setMessage(String message) {
			m_message = message;
		}
	}

	public enum SourceType {
		BROKER("broker"), METASERVER("metaserver");

		public String m_name;

		private SourceType(String name) {
			this.m_name = name;
		}

		public String getName() {
			return m_name;
		}

		public String toString() {
			return this.m_name;
		}
	}

}
