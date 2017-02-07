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

	@Value("${kpi.tts.error.limit.minute:3}")
	private int kpiTtsErrorLimitMinute;

	@Value("${kpi.tts.start.hour:22}")
	private int kpiTtsStartHour;

	@Value("${kpi.tts.last.hours:10}")
	private int kpiTtsLastHours;

	@Value("${monitor.checker.enable:true}")
	private boolean monitorCheckerEnable;

	@Value("${monitor.checker.notify.enable:true}")
	private boolean monitorCheckerNotifyEnable;

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

	public String getZabbixUrl1() {
		return zabbixUrl1;
	}

	public void setZabbixUrl1(String zabbixUrl1) {
		this.zabbixUrl1 = zabbixUrl1;
	}

	public int getKpiTtsStartHour() {
		return kpiTtsStartHour;
	}

	public void setKpiTtsStartHour(int kpiTtsStartHour) {
		this.kpiTtsStartHour = kpiTtsStartHour;
	}

	public int getKpiTtsLastHours() {
		return kpiTtsLastHours;
	}

	public void setKpiTtsLastHours(int kpiTtsLastHours) {
		this.kpiTtsLastHours = kpiTtsLastHours;
	}

	public int getKpiTtsErrorLimitMinute() {
		return kpiTtsErrorLimitMinute;
	}

	public void setKpiTtsErrorLimitMinute(int kpiTtsErrorLimitMinute) {
		this.kpiTtsErrorLimitMinute = kpiTtsErrorLimitMinute;
	}

	public boolean isMonitorCheckerEnable() {
		return monitorCheckerEnable;
	}

	public void setMonitorCheckerEnable(boolean monitorCheckerEnable) {
		this.monitorCheckerEnable = monitorCheckerEnable;
	}

	public boolean isMonitorCheckerNotifyEnable() {
		return monitorCheckerNotifyEnable;
	}

	public void setMonitorCheckerNotifyEnable(boolean monitorCheckerNotifyEnable) {
		this.monitorCheckerNotifyEnable = monitorCheckerNotifyEnable;
	}
}
