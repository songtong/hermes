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

	@Value("${hermes.metaserver.list.url:http://meta.hermes.fws.qa.nt.ctripcorp.com/metaserver/servers}")
	private String metaserverListUrl;

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

	@Value("${produce.transport.failedratio.checker.excluded.topics:[\"All\"]}")
	private String produceTransportFailedRatioCheckerExcludedTopics;

	@Value("${meta.rest.url:http://meta.hermes.fx.ctripcorp.com/meta/complete}")
	private String metaRestUrl;

	@Value("${produce.acktriedratio.checker.noproducetried.acked.threshold:100}")
	private int produceAckedTriedRatioAckedCountThresholdWhileNoProduceTried;

	@Value("${produce.acktriedratio.checker.threshold:1}")
	private double produceAckedTriedRatioThreshold;

	@Value("${produce.send.cmd.failedratio.checker.threshold:0.5}")
	private double produceTransportFailedRatioThreshold;

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
	
	@Value("${zabbix1.url:${zabbix.url}}")
	private String zabbixUrl1;

	@Value("${zabbix.username:guest}")
	private String zabbixUsername;

	@Value("${zabbix.password:}")
	private String zabbixPassword;

	@Value("${broker.log.error.checker.threshold:1}")
	private int brokerErrorThreshold;

	@Value("${metaserver.log.error.checker.threshold:1}")
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
	@Value("${partition.checker.include.topics.retain.hour:{\".*\":240}}")
	private String partitionRetainInHour;

	@Value("${partition.checker.exclude.topics:[]}")
	private String partitionCheckerExcludeTopics;

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

	/**
	 * { ".*" : {-1:10}, "song.test" : {-1:100} } in minute
	 */
	@Value("${long.time.no.produce.checker.include.topics:{\".*\": {-1:30}}}")
	private String longTimeNoProduceCheckerIncludeTopics;

	/**
	 * [ "song.test" ]
	 */
	@Value("${long.time.no.produce.checker.exclude.topics:[\"cmessage_fws\"]}")
	private String longTimeNoProduceCheckerExcludeTopics;

	/**
	 * { ".*" : {"song.test.*": 30}, "song.test" : {"song.test.group": 100} } in minute
	 */
	@Value("${long.time.no.consume.checker.include.consumers:{\"hotel..*\": {\".*\": 60}}}")
	private String longTimeNoConsumeCheckerIncludeConsumers;

	/**
	 * {"song.test":["song.test.group"], ".*":[".*"]}
	 */
	@Value("${long.time.no.consume.checker.exclude.consumers:{\"cmessage_fws\":[\".*\"]}}")
	private String longTimeNoConsumeCheckerExcludeConsumers;

	@Value("${elastic.search.query.host:osg.ops.ctripcorp.com}")
	private String elasticSearchQueryHost;

	@Value("${elastic.search.token.path:/opt/ctrip/data/hermes/hermes-es.token}")
	private String elasticSearchTokenPath;

	@Value("${lease.checker.exclude.topics:[]}")
	private String leaseCheckerExcludeTopics;

	@Value("${lease.acquire.timeout.millisecond.limit:2000}")
	private int leaseAcquireTimeoutMillisecondLimit;

	@Value("${lease.acquire.timeout.count.limit:10}")
	private int leaseAcquireTimeoutCountLimit;

	@Value("${lease.acquire.error.count.limit:10}")
	private int leaseAcquireErrorCountLimit;

	@Value("${lease.renew.timeout.millisecond.limit:2000}")
	private int leaseRenewTimeoutMillisecondLimit;

	@Value("${lease.renew.timeout.count.limit:10}")
	private int leaseRenewTimeoutCountLimit;

	@Value("${lease.renew.error.count.limit:10}")
	private int leaseRenewErrorCountLimit;

	@Value("${meta.request.error.count.limit:10}")
	private int metaRequestErrorCountLimit;

	@Value("${meta.request.timeout.count.limit:10}")
	private int metaRequestTimeoutCountLimit;

	@Value("${meta.request.timeout.millisecond.limit:2000}")
	private int metaRequestTimeoutMillisecondLimit;

	@Value("${consume.ack.cmd.fail.ratio.limit:0.5}")
	private float consumeAckCmdFailRatioLimit;

	@Value("${consume.ack.cmd.fail.ratio.exclude.consumers:[]}")
	private String consumeAckCmdFailRatioExcludeConsumers;

	@Value("${hermes.metaserver.cat.domain:hermes-metaserver}")
	private String hermesMetaserverCatDomain;

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

	public String getProduceTransportFailedRatioCheckerExcludedTopics() {
		return produceTransportFailedRatioCheckerExcludedTopics;
	}

	public void setProduceTransportFailedRatioCheckerExcludedTopics(
	      String produceTransportRailedRatioCheckerExcludedTopics) {
		this.produceTransportFailedRatioCheckerExcludedTopics = produceTransportRailedRatioCheckerExcludedTopics;
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

	public String getPartitionRetainInHour() {
		return partitionRetainInHour;
	}

	public void setPartitionRetainInHour(String partitionRetainInHour) {
		this.partitionRetainInHour = partitionRetainInHour;
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

	public int getResendPartitionSizeIncrementStep() {
		return partitionSizeIncrementStep / 10;
	}

	public void setPartitionSizeIncrementStep(int partitionSizeIncrementStep) {
		this.partitionSizeIncrementStep = partitionSizeIncrementStep;
	}

	public String getPartitionCheckerExcludeTopics() {
		return partitionCheckerExcludeTopics;
	}

	public void setPartitionCheckerExcludeTopics(String partitionCheckerExcludeTopics) {
		this.partitionCheckerExcludeTopics = partitionCheckerExcludeTopics;
	}

	public String getLongTimeNoProduceCheckerIncludeTopics() {
		return longTimeNoProduceCheckerIncludeTopics;
	}

	public void setLongTimeNoProduceCheckerIncludeTopics(String longTimeNoProduceCheckerIncludeTopics) {
		this.longTimeNoProduceCheckerIncludeTopics = longTimeNoProduceCheckerIncludeTopics;
	}

	public String getLongTimeNoProduceCheckerExcludeTopics() {
		return longTimeNoProduceCheckerExcludeTopics;
	}

	public void setLongTimeNoProduceCheckerExcludeTopics(String longTimeNoProduceCheckerExcludeTopics) {
		this.longTimeNoProduceCheckerExcludeTopics = longTimeNoProduceCheckerExcludeTopics;
	}

	public double getProduceTransportFailedRatioThreshold() {
		return produceTransportFailedRatioThreshold;
	}

	public void setProduceTransportFailedRatioThreshold(double produceTransportFailedRatioThreshold) {
		this.produceTransportFailedRatioThreshold = produceTransportFailedRatioThreshold;
	}

	public String getElasticSearchQueryHost() {
		return elasticSearchQueryHost;
	}

	public void setElasticSearchQueryHost(String elasticSearchQueryHost) {
		this.elasticSearchQueryHost = elasticSearchQueryHost;
	}

	public String getElasticSearchTokenPath() {
		return elasticSearchTokenPath;
	}

	public void setElasticSearchTokenPath(String elasticSearchTokenPath) {
		this.elasticSearchTokenPath = elasticSearchTokenPath;
	}

	public String getLeaseCheckerExcludeTopics() {
		return leaseCheckerExcludeTopics;
	}

	public void setLeaseCheckerExcludeTopics(String leaseCheckerExcludeTopics) {
		this.leaseCheckerExcludeTopics = leaseCheckerExcludeTopics;
	}

	public int getLeaseAcquireErrorCountLimit() {
		return leaseAcquireErrorCountLimit;
	}

	public void setLeaseAcquireErrorCountLimit(int leaseAcquireErrorCountLimit) {
		this.leaseAcquireErrorCountLimit = leaseAcquireErrorCountLimit;
	}

	public int getLeaseRenewErrorCountLimit() {
		return leaseRenewErrorCountLimit;
	}

	public void setLeaseRenewErrorCountLimit(int leaseRenewErrorCountLimit) {
		this.leaseRenewErrorCountLimit = leaseRenewErrorCountLimit;
	}

	public int getLeaseAcquireTimeoutMillisecondLimit() {
		return leaseAcquireTimeoutMillisecondLimit;
	}

	public void setLeaseAcquireTimeoutMillisecondLimit(int leaseAcquireTimeoutMillisecondLimit) {
		this.leaseAcquireTimeoutMillisecondLimit = leaseAcquireTimeoutMillisecondLimit;
	}

	public int getLeaseAcquireTimeoutCountLimit() {
		return leaseAcquireTimeoutCountLimit;
	}

	public void setLeaseAcquireTimeoutCountLimit(int leaseAcquireTimeoutCountLimit) {
		this.leaseAcquireTimeoutCountLimit = leaseAcquireTimeoutCountLimit;
	}

	public int getLeaseRenewTimeoutMillisecondLimit() {
		return leaseRenewTimeoutMillisecondLimit;
	}

	public void setLeaseRenewTimeoutMillisecondLimit(int leaseRenewTimeoutMillisecondLimit) {
		this.leaseRenewTimeoutMillisecondLimit = leaseRenewTimeoutMillisecondLimit;
	}

	public int getLeaseRenewTimeoutCountLimit() {
		return leaseRenewTimeoutCountLimit;
	}

	public void setLeaseRenewTimeoutCountLimit(int leaseRenewTimeoutCountLimit) {
		this.leaseRenewTimeoutCountLimit = leaseRenewTimeoutCountLimit;
	}

	public int getMetaRequestErrorCountLimit() {
		return metaRequestErrorCountLimit;
	}

	public void setMetaRequestErrorCountLimit(int metaRequestErrorCountLimit) {
		this.metaRequestErrorCountLimit = metaRequestErrorCountLimit;
	}

	public int getMetaRequestTimeoutCountLimit() {
		return metaRequestTimeoutCountLimit;
	}

	public void setMetaRequestTimeoutCountLimit(int metaRequestTimeoutCountLimit) {
		this.metaRequestTimeoutCountLimit = metaRequestTimeoutCountLimit;
	}

	public int getMetaRequestTimeoutMillisecondLimit() {
		return metaRequestTimeoutMillisecondLimit;
	}

	public void setMetaRequestTimeoutMillisecondLimit(int metaRequestTimeoutMillisecondLimit) {
		this.metaRequestTimeoutMillisecondLimit = metaRequestTimeoutMillisecondLimit;
	}

	public float getConsumeAckCmdFailRatioLimit() {
		return consumeAckCmdFailRatioLimit;
	}

	public void setConsumeAckCmdFailRatioLimit(float consumeAckCmdFailRatioLimit) {
		this.consumeAckCmdFailRatioLimit = consumeAckCmdFailRatioLimit;
	}

	public String getConsumeAckCmdFailRatioExcludeConsumers() {
		return consumeAckCmdFailRatioExcludeConsumers;
	}

	public void setConsumeAckCmdFailRatioExcludeConsumers(String consumeAckCmdFailRatioExcludeConsumers) {
		this.consumeAckCmdFailRatioExcludeConsumers = consumeAckCmdFailRatioExcludeConsumers;
	}

	public String getLongTimeNoConsumeCheckerIncludeConsumers() {
		return longTimeNoConsumeCheckerIncludeConsumers;
	}

	public void setLongTimeNoConsumeCheckerIncludeConsumers(String longTimeNoConsumeCheckerIncludeConsumers) {
		this.longTimeNoConsumeCheckerIncludeConsumers = longTimeNoConsumeCheckerIncludeConsumers;
	}

	public String getLongTimeNoConsumeCheckerExcludeConsumers() {
		return longTimeNoConsumeCheckerExcludeConsumers;
	}

	public void setLongTimeNoConsumeCheckerExcludeConsumers(String longTimeNoConsumeCheckerExcludeConsumers) {
		this.longTimeNoConsumeCheckerExcludeConsumers = longTimeNoConsumeCheckerExcludeConsumers;
	}

	public String getMetaserverListUrl() {
		return metaserverListUrl;
	}

	public void setMetaserverListUrl(String metaserverListUrl) {
		this.metaserverListUrl = metaserverListUrl;
	}

	public String getHermesMetaserverCatDomain() {
		return hermesMetaserverCatDomain;
	}

	public void setHermesMetaserverCatDomain(String hermesMetaserverCatDomain) {
		this.hermesMetaserverCatDomain = hermesMetaserverCatDomain;
	}

	public String getZabbixUrl1() {
	   return zabbixUrl1;
   }

	public void setZabbixUrl1(String zabbixUrl1) {
	   this.zabbixUrl1 = zabbixUrl1;
   }
}
