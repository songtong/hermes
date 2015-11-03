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

	@Value("${produce.ackedtriedratio.checker.excluded.topics:[\"All\"]}")
	private String produceAckedTriedRatioCheckerExcludedTopics;

	@Value("${meta.rest.url:http://meta.hermes.fx.ctripcorp.com/meta}")
	private String metaRestUrl;

	@Value("${produce.acktriedratio.checker.noproducetried.acked.threshold:100}")
	private int produceAckedTriedRatioAckedCountThresholdWhileNoProduceTried;

	@Value("${produce.acktriedratio.checker.threshold:0.3}")
	private double produceAckedTriedRatioThreshold;

	@Value("${zabbix.kafka.broker.hosts}")
	private String[] zabbixKafkaBrokerHosts;

	@Value("${zabbix.mysql.broker.hosts}")
	private String[] zabbixMysqlBrokerHosts;

	@Value("${zabbix.metaserver.hosts}")
	private String[] zabbixMetaserverHosts;

	@Value("${zabbix.portal.hosts}")
	private String[] zabbixPortalHosts;

	@Value("${zabbix.zookeeper.hosts}")
	private String[] zabbixZookeeperHosts;

	@Value("${zabbix.url}")
	private String zabbixUrl;
	
	@Value("${zabbix.username:guest}")
	private String zabbixUsername;
	
	@Value("${zabbix.password:}")
	private String zabbixPassword;
	
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

	public String getProduceAckedTriedRatioCheckerExcludedTopics() {
		return produceAckedTriedRatioCheckerExcludedTopics;
	}

	public void setProduceAckedTriedRatioCheckerExcludedTopics(String produceAckedTriedRatioCheckerExcludedTopics) {
		this.produceAckedTriedRatioCheckerExcludedTopics = produceAckedTriedRatioCheckerExcludedTopics;
	}

	public String getMetaRestUrl() {
		return metaRestUrl;
	}

	public void setMetaRestUrl(String metaRestUrl) {
		this.metaRestUrl = metaRestUrl;
	}

	public int getProduceAckedTriedRatioAckedCountThresholdWhileNoProduceTried() {
		return produceAckedTriedRatioAckedCountThresholdWhileNoProduceTried;
	}

	public void setProduceAckedTriedRatioAckedCountThresholdWhileNoProduceTried(
	      int produceAckedTriedRatioAckedCountThresholdWhileNoProduceTried) {
		this.produceAckedTriedRatioAckedCountThresholdWhileNoProduceTried = produceAckedTriedRatioAckedCountThresholdWhileNoProduceTried;
	}

	public double getProduceAckedTriedRatioThreshold() {
		return produceAckedTriedRatioThreshold;
	}

	public void setProduceAckedTriedRatioThreshold(double produceAckedTriedRatioThreshold) {
		this.produceAckedTriedRatioThreshold = produceAckedTriedRatioThreshold;
	}

	public String[] getZabbixZookeeperHosts() {
		return zabbixZookeeperHosts;
	}

	public void setZabbixZookeeperHosts(String[] zabbixZookeeperHosts) {
		this.zabbixZookeeperHosts = zabbixZookeeperHosts;
	}

	public String[] getZabbixPortalHosts() {
		return zabbixPortalHosts;
	}

	public void setZabbixPortalHosts(String[] zabbixPortalHosts) {
		this.zabbixPortalHosts = zabbixPortalHosts;
	}

	public String[] getZabbixMetaserverHosts() {
		return zabbixMetaserverHosts;
	}

	public void setZabbixMetaserverHosts(String[] zabbixMetaserverHosts) {
		this.zabbixMetaserverHosts = zabbixMetaserverHosts;
	}

	public String[] getZabbixMysqlBrokerHosts() {
		return zabbixMysqlBrokerHosts;
	}

	public void setZabbixMysqlBrokerHosts(String[] zabbixMysqlBrokerHosts) {
		this.zabbixMysqlBrokerHosts = zabbixMysqlBrokerHosts;
	}

	public String[] getZabbixKafkaBrokerHosts() {
		return zabbixKafkaBrokerHosts;
	}

	public void setZabbixKafkaBrokerHosts(String[] zabbixKafkaBrokerHosts) {
		this.zabbixKafkaBrokerHosts = zabbixKafkaBrokerHosts;
	}

	public String getZabbixUrl() {
	   return zabbixUrl;
   }

	public void setZabbixUrl(String zabbixUrl) {
	   this.zabbixUrl = zabbixUrl;
   }

	public String getZabbixUsername() {
	   return zabbixUsername;
   }

	public void setZabbixUsername(String zabbixUsername) {
	   this.zabbixUsername = zabbixUsername;
   }

	public String getZabbixPassword() {
	   return zabbixPassword;
   }

	public void setZabbixPassword(String zabbixPassword) {
	   this.zabbixPassword = zabbixPassword;
   }
}
