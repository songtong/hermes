package com.ctrip.hermes.monitor.kafka;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.MetricName;

public abstract class Parser {

	static final Logger log = LoggerFactory.getLogger(Parser.class);

	protected String name;

	protected Map<String,String> tags;

	public String getName() {
		return name;
	}

	public Map<String,String> getTags() {
		return tags;
	}

	public abstract void parse(MetricName metricName);

}