package com.ctrip.hermes.monitor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:hermes.properties")
public class MonitorConfig {

	@Value("${es.cluster.name}")
	private String esClusterName;

	@Value("${es.transport.address}")
	private String[] esTransportAddress;

	@Value("${cat.base.url:http://cat.ctripcorp.com}")
	private String catBaseUrl;

	@Value("${cat.cross.transaction.url.pattern:/cat/r/t?op=graphs&domain=All&date=%s&ip=All&type=%s&forceDownload=xml}")
	private String catCrossTransactionUrlPattern;

	@Value("${cat.event.url.pattern:/cat/r/e?op=graphs&domain=%s&date=%s&type=%s&ip=All&forceDownload=xml}")
	private String catEventUrlPattern;

	@Value("${cat.connect.timeout:10000}")
	private int catConnectTimeout;

	@Value("${cat.read.timeout:30000}")
	private int catReadTimeout;

	@Value("${produce.latency.checker.excluded.topics:[\"All\"]}")
	private String produceLatencyCheckerExcludedTopics;

	@Value("${produce.latency.checker.threshold:1000}")
	private double produceLatencyThreshold;

	@Value("${consume.delay.checker.thresholds:{}}")
	private String consumeDelayThresholds;

	@Value("${produce.failure.checker.excluded.topics:[\"All\"]}")
	private String produceFailureCheckerExcludedTopics;

	@Value("${produce.failure.checker.threshold:10}")
	private int produceFailureCountThreshold;

	public String getEsClusterName() {
		return esClusterName;
	}

	public void setEsClusterName(String esClusterName) {
		this.esClusterName = esClusterName;
	}

	public String[] getEsTransportAddress() {
		return esTransportAddress;
	}

	public void setEsTransportAddress(String[] esTransportAddress) {
		this.esTransportAddress = esTransportAddress;
	}

	public String getCatBaseUrl() {
		return catBaseUrl;
	}

	public void setCatBaseUrl(String catBaseUrl) {
		this.catBaseUrl = catBaseUrl;
	}

	public String getCatCrossTransactionUrlPattern() {
		return catCrossTransactionUrlPattern;
	}

	public void setCatCrossTransactionUrlPattern(String catCrossTransactionUrlPattern) {
		this.catCrossTransactionUrlPattern = catCrossTransactionUrlPattern;
	}

	public int getCatConnectTimeout() {
		return catConnectTimeout;
	}

	public void setCatConnectTimeout(int catConnectTimeout) {
		this.catConnectTimeout = catConnectTimeout;
	}

	public int getCatReadTimeout() {
		return catReadTimeout;
	}

	public void setCatReadTimeout(int catReadTimeout) {
		this.catReadTimeout = catReadTimeout;
	}

	public String getProduceLatencyCheckerExcludedTopics() {
		return produceLatencyCheckerExcludedTopics;
	}

	public void setProduceLatencyCheckerExcludedTopics(String produceLatencyCheckerExcludedTopics) {
		this.produceLatencyCheckerExcludedTopics = produceLatencyCheckerExcludedTopics;
	}

	public double getProduceLatencyThreshold() {
		return produceLatencyThreshold;
	}

	public void setProduceLatencyThreshold(double produceLatencyThreshold) {
		this.produceLatencyThreshold = produceLatencyThreshold;
	}

	public String getConsumeDelayThresholds() {
		return consumeDelayThresholds;
	}

	public void setConsumeDelayThresholds(String consumeDelayThresholds) {
		this.consumeDelayThresholds = consumeDelayThresholds;
	}

	public String getProduceFailureCheckerExcludedTopics() {
		return produceFailureCheckerExcludedTopics;
	}

	public void setProduceFailureCheckerExcludedTopics(String produceFailureCheckerExcludedTopics) {
		this.produceFailureCheckerExcludedTopics = produceFailureCheckerExcludedTopics;
	}

	public String getCatEventUrlPattern() {
		return catEventUrlPattern;
	}

	public void setCatEventUrlPattern(String catEventUrlPattern) {
		this.catEventUrlPattern = catEventUrlPattern;
	}

	public int getProduceFailureCountThreshold() {
		return produceFailureCountThreshold;
	}

	public void setProduceFailureCountThreshold(int produceFailureCountThreshold) {
		this.produceFailureCountThreshold = produceFailureCountThreshold;
	}

}
