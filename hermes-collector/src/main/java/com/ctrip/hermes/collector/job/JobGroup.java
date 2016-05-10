package com.ctrip.hermes.collector.job;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

public enum JobGroup {
	DAEMON("daemon", "daemon job"),
	DEFAULT("default", "defaul group for un-annotated job"), 
	BIZ("biz", "group for biz collector job"), 
	SERVICE("service", "group for service error monitoring job"), 
	REPORT("report", "group for reporting job");
	
	private String m_name;
	private String m_description;
	
	private JobGroup(String name, String description) {
		this.m_name = name;
		this.m_description = description;
	}

	public String getName() {
		return m_name;
	}

	public String getDescription() {
		return m_description;
	}
	
	@JsonCreator
	public static JobGroup forValue(String jobGroup) {
		for (JobGroup group : JobGroup.values()) {
			if (group.getName().equals(jobGroup)) {
				return group;
			}
		}
		return null;
	}
	
	@JsonValue
	public String toString() {
		return this.getName();
	}
	
}
