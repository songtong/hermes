package com.ctrip.hermes.metaservice.monitor.service;

import java.util.List;

import com.ctrip.hermes.metaservice.model.ConsumerMonitorConfig;
import com.ctrip.hermes.metaservice.model.ProducerMonitorConfig;

public interface MonitorConfigService {
	public ProducerMonitorConfig getProducerMonitorConfig(String topic);

	public List<ProducerMonitorConfig> listProducerMonitorConfig();

	public ConsumerMonitorConfig getConsumerMonitorConfig(String topic, String consumer);

	public List<ConsumerMonitorConfig> getConsumerMonitorConfig(String topic);

	public List<ConsumerMonitorConfig> listConsumerMonitorConfig();

	public void setProducerMonitorConfig(ProducerMonitorConfig config);

	public void setConsumerMonitorConfig(ConsumerMonitorConfig config);

	public void deleteProducerMonitorConfig(String topic);

	public void deleteConsumerMonitorConfig(String topic, String consumer);
}
