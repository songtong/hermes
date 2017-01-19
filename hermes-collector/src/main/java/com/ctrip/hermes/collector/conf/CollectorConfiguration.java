package com.ctrip.hermes.collector.conf;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:hermes.properties")
public class CollectorConfiguration {
	// Trashed
	@Value("${datahub.queue.size:100000}")
	private int m_datahubQueueSize;

	@Value("${datahub.threads.count:1000}")
	private int m_datahubThreadsCount;

	@Value("${datahub.retry.waittime:3000}")
	private long m_retryWaitTime;
	
	// Storage Manager config
	
	@Value("${storage.queue.size:500000}")
	private int m_storageQueueSize;

	@Value("${storage.threads.count:1000}")
	private int m_storageThreadsCount;

	@Value("${storage.kafka.topic:fx.hermes.collector.state}")
	private String m_storageTopic;
	
	// Job Executor config
	
	@Value("${executor.thread.count:5}")
	private int m_executorPoolSize;

	@Value("${http.es.name}")
	private String m_esDatsourceName;

	@Value("${http.es.url}")
	private String m_esDatasourceApi;

	@Value("${http.es.token}")
	private String m_esTokenFile;

	@Value("${statehub.queue.size:100000}")
	private int m_statehubQueueSize;

	@Value("${ruleengine.queue.size:100000}")
	private int m_ruleEngineQueueSize;

	@Value("${collector.retry.interval:3000}")
	private int m_collectorRetryInterval;

	@Value("${kafka.brokergroup.default:kafka-default}")
	private String m_defaultKafkaBrokerGroup;

	@Value("${es.ignore.mysql.broker.messages:[]}")
	private String m_esIgnoreMysqlBrokerMessages;

	@Value("${es.ignore.kafka.broker.messages:[\"NOT_LEADER_FOR_PARTITION\",\"Failed to append messages\"]}")
	private String m_esIgnoreKafkaBrokerMessages;

	@Value("${es.ignore.metaserver.messages:[\"SEVERE: An I/O error has occurred while writing a response message entity to the container output stream\"]}")
	private String m_esIgnoreMetaserverMessages;

	@Value("${job.status.log.enabled:true}")
	private boolean m_enableLogJobStatus;

	@Value("${cat.base.api:http://cat.ctripcorp.com}")
	private String m_catBaseApi;

	@Value("${cat.transaction.api.pattern:/cat/r/t?op=graphs&domain=%s&date=%s&ip=All&type=%s&forceDownload=xml}")
	private String m_catTransactionApiPattern;

	@Value("${cat.event.api.pattern:/cat/r/e?op=graphs&domain=%s&date=%s&ip=%s&type=%s&forceDownload=xml}")
	private String m_catEventApiPattern;

	@Value("${cat.produce.latency.type:Message.Produce.Elapse.Large}")
	private String m_catProduceLatencyType;
	
	// Real time alert config.
	
	@Value("${realtime.alert.kafka.enabled:false}")
	private boolean m_enableKafkaRealtimeAlert;
	
	@Value("${realtime.alert.longtimenoproudce.enabled:true}")
	private boolean m_enableLongTimeNoProduceRealtimeAlert;
	
	@Value("${realtime.alert.longtimenoconsume.enabled:true}")
	private boolean m_enableLongTimeNoConsumeRealtimeAlert;
	
	@Value("${realtime.alert.consumelargebacklog.enabled:true}")
	private boolean m_enableConsumeLargeBacklogRealtimeAlert;
	
	@Value("${realtime.alert.commanddrop.enabled:true}")
	private boolean m_enableCommandDropRealtimeAlert;
	
	@Value("${realtime.alert.deadletter.enabled:true}")
	private boolean m_enableDeadLetterRealtimeAlert;
	
	@Value("${realtime.alert.producelargelatency.enabled:true}")
	private boolean m_enableProduceLargeLatencyRealtimeAlert;
	
	@Value("${realtime.alert.serviceerror.enabled:true}")
	private boolean m_enableServiceErrorRealtimeAlert;
	
	@Value("${realtime.alert.consumelargebacklog.open:true}")
	private boolean m_defaultConsumeLargeBacklogAlertOpen;
	
	@Value("${realtime.alert.longtimenoproduce.open:false}")
	private boolean m_defaultLongTimeNoProduceAlertOpen;

	@Value("${realtime.alert.longtimenoconsume.open:false}")
	private boolean m_defaultLongTimeNoConsumeAlertOpen;
	
	@Value("${realtime.alert.deadletter.open:true}")
	private boolean m_defaultDeadLetterAlertOpen;

	@Value("${job.reschedule.enabled:true}")
	private boolean m_enableJobReschedule;
	
	@Value("${realtime.alert.producelargelatency.ratio:0.1}")
	private double m_defaultProduceLatencyAlertRatio;

	@Value("${broker.servers}")
	private String m_brokerServers;

	@Value("${kafka.bootstrap.servers}")
	private String m_kafkaBootstrapServers;

	@Value("${kafka.server.port:9092}")
	private int m_kafkaServerPort;
	
	@Value("${kafka.endpoint.hosts}")
	private String m_kafkaEndPointHosts;

	@Value("${notifier.queue.size:1000}")
	private int m_notifierQueueSize;
	
	@Value("${realtime.alert.report.interval:30}")
	private int m_timeIntervalInMinutesForReport;

	@Value("${notifier.default.mail:Rdkjmes@Ctrip.com}")
	private String m_notifierDefaultMail;

	@Value("${notifier.dev.mail:lxteng@Ctrip.com,qingyang@Ctrip.com,song_t@Ctrip.com,jhliang@Ctrip.com}")
	//@Value("${notifier.dev.mail:lxteng@Ctrip.com}")
	private String m_notifierDevMail;
	
	@Value("${notifier.dev.mail:lxteng@Ctrip.com,qingyang@Ctrip.com,song_t@Ctrip.com,jhliang@Ctrip.com,q_gu@Ctrip.com,wuqm@Ctrip.com}")
	private String m_notifierReportMail;
	
	@Value("${notifier.dev.phone:15216706100}")
	private String m_notifierDevSMSNumber;

	@Value("${realtime.alert.enabled:true}")
	private boolean m_enableRealtimeAlert;

	@Value("${monitor.event.enabled:true}")
	private boolean m_enableMonitorEvent;

	@Value("${job.manual.token}")
	private String m_jobManualToken;

	@Value("${scheduler.pool.size:7}")
	private int m_schedulerPoolSize;
	
	@Value("${backlog.alarm.limit.default:10000}")
	private long m_defaultBacklogAlarmLimit;
	
	@Value("${consume.alarm.timelimit.default:600000}")
	private long m_defaultLongTimeNoConsumeLimit;
	
	@Value("${consume.alarm.timelimit.max:604800000}")
	private long m_defaultLongTimeNoConsumeMaxLimit;
	
	@Value("${produce.alarm.timelimit.default:600000}")
	private long m_defaultLongTimeNoProduceLimit;
	
	@Value("${produce.alarm.timelimit.max:604800000}")
	private long m_defaultLongTimeNoProduceMaxLimit;
	
	@Value("${tts.number.default:15267014652,15216706100,18721960052,15021290572}")
	private String m_defaultTTSNumber;
	
	@Value("${sms.number.default:15267014652,15216706100,18721960052,15021290572}")
	private String m_defaultSMSNumber;
	
	@Value("${mail.frequency.interval.default:5}")
	private int m_defaultMailFrequencyInterval;
	
	@Value("${sms.frequency.interval.default:30}")
	private int m_defaultSmsFrequencyInterval;
	
	@Value("${app.debug:false}")
	private boolean m_debugMode; 
	
	@Value("${topic.max:10000}")
	private int m_maxTopicNumber;
	
	@Value("${topic.partition.max:100}")
	private int m_maxTopicPartitionNumber;
	
	@Value("${topic.consumer.ips.max:100}")
	private int m_maxConsumerIps;
	
	@Value("${topic.consumer.max:100}")
	private int m_maxConsumerNumber;
	
	@Value("${topic.producer.ips.max:100}")
	private int m_maxProduceIps;
	
	@Value("${hickwall.servers}")
	private String m_hickwallServers;
	
	@Value("${hickwall.endpoint}")
	private String m_hickwallEndpoint;
	
	@Value("${hickwall.send.timeout:1000}")
	private long m_hickwallSendTimeout;

	public int getDatahubQueueSize() {
		return m_datahubQueueSize;
	}

	public void setDatahubQueueSize(int datahubQueueSize) {
		m_datahubQueueSize = datahubQueueSize;
	}

	public int getDatahubThreadsCount() {
		return m_datahubThreadsCount;
	}

	public void setDatahubThreadsCount(int datahubThreadsCount) {
		m_datahubThreadsCount = datahubThreadsCount;
	}

	public long getRetryWaitTime() {
		return m_retryWaitTime;
	}

	public void setRetryWaitTime(long retryWaitTime) {
		m_retryWaitTime = retryWaitTime;
	}

	public int getStorageQueueSize() {
		return m_storageQueueSize;
	}

	public void setStorageQueueSize(int storageQueueSize) {
		m_storageQueueSize = storageQueueSize;
	}

	public int getStorageThreadsCount() {
		return m_storageThreadsCount;
	}

	public void setStorageThreadsCount(int storageThreadsCount) {
		m_storageThreadsCount = storageThreadsCount;
	}

	public String getEsDatsourceName() {
		return m_esDatsourceName;
	}

	public void setEsDatsourceName(String esDatsourceName) {
		m_esDatsourceName = esDatsourceName;
	}

	public String getEsDatasourceApi() {
		return m_esDatasourceApi;
	}

	public void setEsDatasourceApi(String esDatasourceApi) {
		m_esDatasourceApi = esDatasourceApi;
	}

	public String getEsTokenFile() {
		return m_esTokenFile;
	}

	public void setEsTokenFile(String esTokenFile) {
		m_esTokenFile = esTokenFile;
	}

	public int getStatehubQueueSize() {
		return m_statehubQueueSize;
	}

	public void setStatehubQueuesize(int statehubQueueSize) {
		m_statehubQueueSize = statehubQueueSize;
	}

	public String getStorageTopic() {
		return m_storageTopic;
	}

	public void setStorageTopic(String storageTopic) {
		m_storageTopic = storageTopic;
	}

	public void setStatehubQueueSize(int statehubQueueSize) {
		m_statehubQueueSize = statehubQueueSize;
	}

	public int getRuleEngineQueueSize() {
		return m_ruleEngineQueueSize;
	}

	public void setRuleEngineQueueSize(int ruleEngineQueueSize) {
		m_ruleEngineQueueSize = ruleEngineQueueSize;
	}

	public int getCollectorRetryInterval() {
		return m_collectorRetryInterval;
	}

	public void setCollectorRetryInterval(int collectorRetryInterval) {
		m_collectorRetryInterval = collectorRetryInterval;
	}

	public String getEsIgnoreMysqlBrokerMessages() {
		return m_esIgnoreMysqlBrokerMessages;
	}

	public void setEsIgnoreMysqlBrokerMessages(
			String esIgnoreMysqlBrokerMessages) {
		m_esIgnoreMysqlBrokerMessages = esIgnoreMysqlBrokerMessages;
	}

	public String getEsIgnoreKafkaBrokerMessages() {
		return m_esIgnoreKafkaBrokerMessages;
	}

	public void setEsIgnoreKafkaBrokerMessages(
			String esIgnoreKafkaBrokerMessages) {
		m_esIgnoreKafkaBrokerMessages = esIgnoreKafkaBrokerMessages;
	}

	public String getEsIgnoreMetaserverMessages() {
		return m_esIgnoreMetaserverMessages;
	}

	public void setEsIgnoreMetaserverMessages(String esIgnoreMetaserverMessages) {
		m_esIgnoreMetaserverMessages = esIgnoreMetaserverMessages;
	}

	public String getDefaultKafkaBrokerGroup() {
		return m_defaultKafkaBrokerGroup;
	}

	public void setDefaultKafkaBrokerGroup(String defaultKafkaBrokerGroup) {
		m_defaultKafkaBrokerGroup = defaultKafkaBrokerGroup;
	}

	public boolean isEnableLogJobStatus() {
		return m_enableLogJobStatus;
	}

	public void setEnableLogJobStatus(boolean enableLogJobStatus) {
		m_enableLogJobStatus = enableLogJobStatus;
	}

	public String getCatBaseApi() {
		return m_catBaseApi;
	}

	public void setCatBaseApi(String catBaseApi) {
		m_catBaseApi = catBaseApi;
	}

	public String getCatTransactionApiPattern() {
		return m_catTransactionApiPattern;
	}

	public void setCatTransactionApiPattern(String catTransactionApiPattern) {
		m_catTransactionApiPattern = catTransactionApiPattern;
	}

	public int getExecutorPoolSize() {
		return m_executorPoolSize;
	}

	public void setExecutorPoolSize(int executorPoolSize) {
		m_executorPoolSize = executorPoolSize;
	}

	public String getCatProduceLatencyType() {
		return m_catProduceLatencyType;
	}

	public void setCatProduceLatencyType(String catProduceLatencyType) {
		m_catProduceLatencyType = catProduceLatencyType;
	}

	public boolean isEnableJobReschedule() {
		return m_enableJobReschedule;
	}

	public void setEnableJobReschedule(boolean enableJobReschedule) {
		this.m_enableJobReschedule = enableJobReschedule;
	}

	public String getKafkaBootstrapServers() {
		return m_kafkaBootstrapServers;
	}

	public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
		m_kafkaBootstrapServers = kafkaBootstrapServers;
	}

	public int getKafkaServerPort() {
		return m_kafkaServerPort;
	}

	public void setKafkaServerPort(int kafkaServerPort) {
		m_kafkaServerPort = kafkaServerPort;
	}

	public int getNotifierQueueSize() {
		return m_notifierQueueSize;
	}

	public void setNotifierQueueSize(int notifierQueueSize) {
		m_notifierQueueSize = notifierQueueSize;
	}

	public String getNotifierDefaultMail() {
		if (isDebugMode()) {
			return this.getNotifierDevMail();
		}
		return m_notifierDefaultMail;
	}

	public void setNotifierDefaultMail(String notifierDefaultMail) {
		m_notifierDefaultMail = notifierDefaultMail;
	}

	public boolean isEnableRealtimeAlert() {
		return m_enableRealtimeAlert;
	}

	public void setEnableRealtimeAlert(boolean enableRealtimeAlert) {
		m_enableRealtimeAlert = enableRealtimeAlert;
	}

	public boolean isEnableMonitorEvent() {
		return m_enableMonitorEvent;
	}

	public void setEnableMonitorEvent(boolean enableMonitorEvent) {
		m_enableMonitorEvent = enableMonitorEvent;
	}

	public String getJobManualToken() {
		return m_jobManualToken;
	}

	public void setJobManualToken(String jobManualToken) {
		m_jobManualToken = jobManualToken;
	}

	public int getSchedulerPoolSize() {
		return m_schedulerPoolSize;
	}

	public void setSchedulerPoolSize(int schedulerPoolSize) {
		m_schedulerPoolSize = schedulerPoolSize;
	}

	public String getCatEventApiPattern() {
		return m_catEventApiPattern;
	}

	public void setCatEventApiPattern(String catEventApiPattern) {
		m_catEventApiPattern = catEventApiPattern;
	}

	public String getNotifierDevMail() {
		return m_notifierDevMail;
	}

	public void setNotifierDevMail(String notifierDevMail) {
		m_notifierDevMail = notifierDevMail;
	}

	public String getBrokerServers() {
		return m_brokerServers;
	}

	public void setBrokerServers(String brokerServers) {
		m_brokerServers = brokerServers;
	}

	public String getKafkaEndPointHosts() {
		return m_kafkaEndPointHosts;
	}

	public void setKafkaEndPointHosts(String kafkaEndPointHosts) {
		m_kafkaEndPointHosts = kafkaEndPointHosts;
	}

	public long getDefaultBacklogAlarmLimit() {
		return m_defaultBacklogAlarmLimit;
	}

	public void setDefaultBacklogAlarmLimit(long defaultBacklogAlarmLimit) {
		m_defaultBacklogAlarmLimit = defaultBacklogAlarmLimit;
	}

	public long getDefaultLongTimeNoConsumeLimit() {
		return m_defaultLongTimeNoConsumeLimit;
	}

	public void setDefaultLongTimeNoConsumeLimit(long defaultLongTimeNoConsumeLimit) {
		m_defaultLongTimeNoConsumeLimit = defaultLongTimeNoConsumeLimit;
	}

	public long getDefaultLongTimeNoProduceLimit() {
		return m_defaultLongTimeNoProduceLimit;
	}

	public void setDefaultLongTimeNoProduceLimit(long defaultLongTimeNoProduceLimit) {
		m_defaultLongTimeNoProduceLimit = defaultLongTimeNoProduceLimit;
	}

	public String getDefaultTTSNumber() {
		return m_defaultTTSNumber;
	}

	public void setDefaultTTSNumber(String defaultTTSNumber) {
		m_defaultTTSNumber = defaultTTSNumber;
	}

	public String getDefaultSMSNumber() {
		if (isDebugMode()) {
			return this.getNotifierDevSMSNumber();
		}
		return m_defaultSMSNumber;
	}

	public void setDefaultSMSNumber(String defaultSMSNumber) {
		m_defaultSMSNumber = defaultSMSNumber;
	}

	public boolean isEnableKafkaRealtimeAlert() {
		return m_enableKafkaRealtimeAlert;
	}

	public void setEnableKafkaRealtimeAlert(boolean enableKafkaRealtimeAlert) {
		m_enableKafkaRealtimeAlert = enableKafkaRealtimeAlert;
	}

	public boolean isEnableLongTimeNoProduceRealtimeAlert() {
		return m_enableLongTimeNoProduceRealtimeAlert;
	}

	public void setEnableLongTimeNoProduceRealtimeAlert(
			boolean enableLongTimeNoProduceRealtimeAlert) {
		m_enableLongTimeNoProduceRealtimeAlert = enableLongTimeNoProduceRealtimeAlert;
	}

	public boolean isEnableLongTimeNoConsumeRealtimeAlert() {
		return m_enableLongTimeNoConsumeRealtimeAlert;
	}

	public void setEnableLongTimeNoConsumeRealtimeAlert(
			boolean enableLongTimeNoConsumeRealtimeAlert) {
		m_enableLongTimeNoConsumeRealtimeAlert = enableLongTimeNoConsumeRealtimeAlert;
	}

	public boolean isEnableConsumeLargeBacklogRealtimeAlert() {
		return m_enableConsumeLargeBacklogRealtimeAlert;
	}

	public void setEnableConsumeLargeBacklogRealtimeAlert(
			boolean enableConsumeLargeBacklogRealtimeAlert) {
		m_enableConsumeLargeBacklogRealtimeAlert = enableConsumeLargeBacklogRealtimeAlert;
	}

	public boolean isEnableCommandDropRealtimeAlert() {
		return m_enableCommandDropRealtimeAlert;
	}

	public void setEnableCommandDropRealtimeAlert(
			boolean enableCommandDropRealtimeAlert) {
		m_enableCommandDropRealtimeAlert = enableCommandDropRealtimeAlert;
	}

	public boolean isEnableDeadLetterRealtimeAlert() {
		return m_enableDeadLetterRealtimeAlert;
	}

	public void setEnableDeadLetterRealtimeAlert(
			boolean enableDeadLetterRealtimeAlert) {
		m_enableDeadLetterRealtimeAlert = enableDeadLetterRealtimeAlert;
	}

	public boolean isEnableProduceLargeLatencyRealtimeAlert() {
		return m_enableProduceLargeLatencyRealtimeAlert;
	}

	public void setEnableProduceLargeLatencyRealtimeAlert(
			boolean enableProduceLargeLatencyRealtimeAlert) {
		m_enableProduceLargeLatencyRealtimeAlert = enableProduceLargeLatencyRealtimeAlert;
	}

	public boolean isEnableServiceErrorRealtimeAlert() {
		return m_enableServiceErrorRealtimeAlert;
	}

	public void setEnableServiceErrorRealtimeAlert(
			boolean enableServiceErrorRealtimeAlert) {
		m_enableServiceErrorRealtimeAlert = enableServiceErrorRealtimeAlert;
	}

	public boolean isDefaultConsumeLargeBacklogAlertOpen() {
		return m_defaultConsumeLargeBacklogAlertOpen;
	}

	public void setDefaultConsumeLargeBacklogAlertOpen(
			boolean defaultConsumeLargeBacklogAlertOpen) {
		m_defaultConsumeLargeBacklogAlertOpen = defaultConsumeLargeBacklogAlertOpen;
	}

	public boolean isDefaultLongTimeNoProduceAlertOpen() {
		return m_defaultLongTimeNoProduceAlertOpen;
	}

	public void setDefaultLongTimeNoProduceAlertOpen(
			boolean defaultLongTimeNoProduceAlertOpen) {
		m_defaultLongTimeNoProduceAlertOpen = defaultLongTimeNoProduceAlertOpen;
	}

	public boolean isDefaultLongTimeNoConsumeAlertOpen() {
		return m_defaultLongTimeNoConsumeAlertOpen;
	}

	public void setDefaultLongTimeNoConsumeAlertOpen(
			boolean defaultLongTimeNoConsumeAlertOpen) {
		m_defaultLongTimeNoConsumeAlertOpen = defaultLongTimeNoConsumeAlertOpen;
	}

	public boolean isDefaultDeadLetterAlertOpen() {
		return m_defaultDeadLetterAlertOpen;
	}

	public void setDefaultDeadLetterAlertOpen(boolean defaultDeadLetterAlertOpen) {
		m_defaultDeadLetterAlertOpen = defaultDeadLetterAlertOpen;
	}

	public double getDefaultProduceLatencyAlertRatio() {
		return m_defaultProduceLatencyAlertRatio;
	}

	public void setDefaultProduceLatencyAlertRatio(
			double defaultProduceLatencyAlertRatio) {
		m_defaultProduceLatencyAlertRatio = defaultProduceLatencyAlertRatio;
	}

	public long getDefaultLongTimeNoConsumeMaxLimit() {
		return m_defaultLongTimeNoConsumeMaxLimit;
	}

	public void setDefaultLongTimeNoConsumeMaxLimit(
			long defaultLongTimeNoConsumeMaxLimit) {
		m_defaultLongTimeNoConsumeMaxLimit = defaultLongTimeNoConsumeMaxLimit;
	}

	public long getDefaultLongTimeNoProduceMaxLimit() {
		return m_defaultLongTimeNoProduceMaxLimit;
	}

	public void setDefaultLongTimeNoProduceMaxLimit(
			long defaultLongTimeNoProduceMaxLimit) {
		m_defaultLongTimeNoProduceMaxLimit = defaultLongTimeNoProduceMaxLimit;
	}

	public String getNotifierDevSMSNumber() {
		return m_notifierDevSMSNumber;
	}

	public void setNotifierDevSMSNumber(String notifierDevSMSNumber) {
		m_notifierDevSMSNumber = notifierDevSMSNumber;
	}

	public boolean isDebugMode() {
		return m_debugMode;
	}

	public void setDebugMode(boolean debugMode) {
		m_debugMode = debugMode;
	}

	public String getNotifierReportMail() {
		return m_notifierReportMail;
	}

	public void setNotifierReportMail(String notifierReportMail) {
		m_notifierReportMail = notifierReportMail;
	}

	public int getTimeIntervalInMinutesForReport() {
		return m_timeIntervalInMinutesForReport;
	}

	public void setTimeIntervalInMinutesForReport(int timeIntervalInMinutesForReport) {
		m_timeIntervalInMinutesForReport = timeIntervalInMinutesForReport;
	}

	public int getDefaultMailFrequencyInterval() {
		return m_defaultMailFrequencyInterval;
	}

	public void setDefaultMailFrequencyInterval(int defaultMailFrequencyInterval) {
		m_defaultMailFrequencyInterval = defaultMailFrequencyInterval;
	}

	public int getDefaultSmsFrequencyInterval() {
		return m_defaultSmsFrequencyInterval;
	}

	public void setDefaultSmsFrequencyInterval(int defaultSmsFrequencyInterval) {
		m_defaultSmsFrequencyInterval = defaultSmsFrequencyInterval;
	}

	public int getMaxTopicNumber() {
		return m_maxTopicNumber;
	}

	public void setMaxTopicNumber(int maxTopicNumber) {
		m_maxTopicNumber = maxTopicNumber;
	}

	public int getMaxTopicPartitionNumber() {
		return m_maxTopicPartitionNumber;
	}

	public void setMaxTopicPartitionNumber(int maxTopicPartitionNumber) {
		m_maxTopicPartitionNumber = maxTopicPartitionNumber;
	}

	public int getMaxConsumerIps() {
		return m_maxConsumerIps;
	}

	public void setMaxConsumerIps(int maxConsumerIps) {
		m_maxConsumerIps = maxConsumerIps;
	}

	public int getMaxConsumerNumber() {
		return m_maxConsumerNumber;
	}

	public void setMaxConsumerNumber(int maxConsumerNumber) {
		m_maxConsumerNumber = maxConsumerNumber;
	}

	public int getMaxProduceIps() {
		return m_maxProduceIps;
	}

	public void setMaxProduceIps(int maxProduceIps) {
		m_maxProduceIps = maxProduceIps;
	}

	public String getHickwallServers() {
		return m_hickwallServers;
	}

	public void setHickwallServers(String hickwallServers) {
		m_hickwallServers = hickwallServers;
	}

	public String getHickwallEndpoint() {
		return m_hickwallEndpoint;
	}

	public void setHickwallEndpoint(String hickwallEndpoint) {
		m_hickwallEndpoint = hickwallEndpoint;
	}

	public long getHickwallSendTimeout() {
		return m_hickwallSendTimeout;
	}

	public void setHickwallSendTimeout(long hickwallSendTimeout) {
		m_hickwallSendTimeout = hickwallSendTimeout;
	}
	
}
