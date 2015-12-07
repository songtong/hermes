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

	@Value("${cat.connect.timeout:20000}")
	private int catConnectTimeout;

	@Value("${cat.read.timeout:60000}")
	private int catReadTimeout;

	@Value("${produce.latency.checker.excluded.topics:[\"All\"]}")
	private String produceLatencyCheckerExcludedTopics;

	@Value("${produce.latency.checker.thresholds:{\"Default\":5000}}")
	private String produceLatencyThresholds;

	@Value("${consume.delay.checker.thresholds:{}}")
	private String consumeDelayThresholds;

	@Value("${produce.failure.checker.excluded.topics:[\"All\"]}")
	private String produceFailureCheckerExcludedTopics;

	@Value("${produce.failure.checker.threshold:10}")
	private int produceFailureCountThreshold;

	@Value("${produce.ackedtriedratio.checker.excluded.topics:[\"All\"]}")
	private String produceAckedTriedRatioCheckerExcludedTopics;

	@Value("${meta.rest.url:http://meta.hermes.fx.ctripcorp.com/meta/complete}")
	private String metaRestUrl;

	@Value("${produce.acktriedratio.checker.noproducetried.acked.threshold:100}")
	private int produceAckedTriedRatioAckedCountThresholdWhileNoProduceTried;

	@Value("${produce.acktriedratio.checker.threshold:1}")
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

	@Value("${broker.log.error.checker.threshold:100}")
	private int brokerErrorThreshold;

	@Value("${metaserver.log.error.checker.threshold:20}")
	private int metaserverErrorThreshold;

	/**
	 * { "song.test" : 100, ".*" : 100, "song..*" : 1000 }
	 */
	@Value("${topic.large.dead.letter.checker.include.topics:{\".*\":100}}")
	private String deadLetterCheckerIncludeTopics;

	/**
	 * ["song.test", "song..*"]
	 */
	@Value("${topic.large.dead.letter.checker.exclude.topics:[]}")
	private String deadLetterCheckerExcludeTopics;

	/**
	 * { ".*" : { ".*" : 100}, "song.test" : { "song.test.group" : 100, "song.test.group2" : 1000 }}
	 */
	@Value("${consume.large.backlog.checker.include.topics:{\".*\":{\".*\":2000}}}")
	private String consumeBacklogCheckerIncludeTopics;

	/**
	 * { ".*" : {"song.test..*"} }
	 */
	@Value("${consume.large.backlog.checker.exclude.topics:{}}")
	private String consumeBacklogCheckerExcludeTopics;

	/**
	 * { ".*" : 30, "song.test" : 60, "song..*" : 40 }
	 */
	@Value("${partition.checker.include.topics.retain.day:{\".*\":30}}")
	private String partitionRetainInDay;

	@Value("${partition.checker.watermark.day:10}")
	private int partitionWatermarkInDay;

	@Value("${partition.checker.increment.day:20}")
	private int partitionIncrementInDay;

	@Value("${partition.checker.increment.max.count:100}")
	private int partitionIncrementMaxCount;

	@Value("${partition.checker.increment.partition.max.size:50000000}")
	private int partitionMaxSize;

	@Value("${partition.checker.size.increment.step:1000000}")
	private int partitionSizeIncrementStep;

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

	public String getProduceLatencyThresholds() {
		return produceLatencyThresholds;
	}

	public void setProduceLatencyThresholds(String produceLatencyThresholds) {
		this.produceLatencyThresholds = produceLatencyThresholds;
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

	public int getBrokerErrorThreshold() {
		return brokerErrorThreshold;
	}

	public void setBrokerErrorThreshold(int brokerErrorThreshold) {
		this.brokerErrorThreshold = brokerErrorThreshold;
	}

	public int getMetaserverErrorThreshold() {
		return metaserverErrorThreshold;
	}

	public void setMetaserverErrorThreshold(int metaserverErrorThreshold) {
		this.metaserverErrorThreshold = metaserverErrorThreshold;
	}

	public String getDeadLetterCheckerIncludeTopics() {
		return deadLetterCheckerIncludeTopics;
	}

	public void setDeadLetterCheckerIncludeTopics(String deadLetterCheckerIncludeTopics) {
		this.deadLetterCheckerIncludeTopics = deadLetterCheckerIncludeTopics;
	}

	public String getConsumeBacklogCheckerIncludeTopics() {
		return consumeBacklogCheckerIncludeTopics;
	}

	public void setConsumeBacklogCheckerIncludeTopics(String consumeBacklogCheckerIncludeTopics) {
		this.consumeBacklogCheckerIncludeTopics = consumeBacklogCheckerIncludeTopics;
	}

	public String getDeadLetterCheckerExcludeTopics() {
		return deadLetterCheckerExcludeTopics;
	}

	public void setDeadLetterCheckerExcludeTopics(String deadLetterCheckerExcludeTopics) {
		this.deadLetterCheckerExcludeTopics = deadLetterCheckerExcludeTopics;
	}

	public String getConsumeBacklogCheckerExcludeTopics() {
		return consumeBacklogCheckerExcludeTopics;
	}

	public void setConsumeBacklogCheckerExcludeTopics(String consumeBacklogCheckerExcludeTopics) {
		this.consumeBacklogCheckerExcludeTopics = consumeBacklogCheckerExcludeTopics;
	}

	public String getPartitionRetainInDay() {
		return partitionRetainInDay;
	}

	public void setPartitionRetainInDay(String partitionRetainInDay) {
		this.partitionRetainInDay = partitionRetainInDay;
	}

	public int getPartitionWatermarkInDay() {
		return partitionWatermarkInDay;
	}

	public void setPartitionWatermarkInDay(int partitionWatermarkInDay) {
		this.partitionWatermarkInDay = partitionWatermarkInDay;
	}

	public int getPartitionIncrementInDay() {
		return partitionIncrementInDay;
	}

	public void setPartitionIncrementInDay(int partitionIncrementInDay) {
		this.partitionIncrementInDay = partitionIncrementInDay;
	}

	public int getPartitionIncrementMaxCount() {
		return partitionIncrementMaxCount;
	}

	public void setPartitionIncrementMaxCount(int partitionIncrementMaxCount) {
		this.partitionIncrementMaxCount = partitionIncrementMaxCount;
	}

	public int getPartitionMaxSize() {
		return partitionMaxSize;
	}

	public void setPartitionMaxSize(int partitionMaxSize) {
		this.partitionMaxSize = partitionMaxSize;
	}

	public int getPartitionSizeIncrementStep() {
		return partitionSizeIncrementStep;
	}

	public void setPartitionSizeIncrementStep(int partitionSizeIncrementStep) {
		this.partitionSizeIncrementStep = partitionSizeIncrementStep;
	}
}
