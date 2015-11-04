package com.ctrip.hermes.monitor.domain;

import java.util.Date;
import java.util.Map;

import org.elasticsearch.common.lang3.builder.ToStringBuilder;

public class MonitorItem {

	private String source;

	private String category;

	private Date startDate;

	private Date endDate;

	private Map<String, Object> value;

	private String host;
	
	private String group;

	public String getCategory() {
		return category;
	}

	public Date getEndDate() {
		return endDate;
	}

	public String getHost() {
		return host;
	}

	public String getSource() {
		return source;
	}

	public Date getStartDate() {
		return startDate;
	}

	public Map<String, Object> getValue() {
		return value;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public void setEndDate(Date end) {
		this.endDate = end;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setStartDate(Date start) {
		this.startDate = start;
	}

	public void setValue(Map<String, Object> value) {
		this.value = value;
	}

	public String getGroup() {
	   return group;
   }

	public void setGroup(String group) {
	   this.group = group;
   }

	public String toString(){
		return ToStringBuilder.reflectionToString(this);
	}
}
