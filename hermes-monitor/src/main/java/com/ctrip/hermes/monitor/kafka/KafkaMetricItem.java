package com.ctrip.hermes.monitor.kafka;

import java.util.Date;
import java.util.Map;

import com.yammer.metrics.core.MetricName;

public class KafkaMetricItem {

	private String name;

	private String group;

	private String scope;

	private String type;

	private String parserName;

	private Map<String, String> tags;

	private Map<String, Number> dimensions;

	private Date time;

	private String metricType;

	public KafkaMetricItem(MetricName metricName, Map<String, Number> dimensions, Parser parser, Date time,
	      String metricType) {
		this.setName(metricName.getName());
		this.setGroup(metricName.getGroup());
		this.setScope(metricName.getScope());
		this.setType(metricName.getType());
		this.setDimensions(dimensions);
		this.setTime(time);
		this.setParserName(parser.getName());
		this.setTags(parser.getTags());
		this.setMetricType(metricType);
	}

	public Map<String, Number> getDimensions() {
		return dimensions;
	}

	public String getGroup() {
		return group;
	}

	public String getMetricType() {
		return metricType;
	}

	public String getName() {
		return name;
	}

	public String getParserName() {
		return parserName;
	}

	public String getScope() {
		return scope;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public Date getTime() {
		return time;
	}

	public String getType() {
		return type;
	}

	public void setDimensions(Map<String, Number> dimensions) {
		this.dimensions = dimensions;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public void setMetricType(String metricType) {
		this.metricType = metricType;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setParserName(String parserName) {
		this.parserName = parserName;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public void setType(String type) {
		this.type = type;
	}

}
