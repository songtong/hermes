package com.ctrip.hermes.metaservice.monitor.service;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.metaservice.model.ProducerMonitorConfig;

public class DefaultMonitorConfigServiceTest extends ComponentTestCase {

	@Test
	public void test() {
		MonitorConfigService configService = lookup(MonitorConfigService.class);
		ProducerMonitorConfig producerMonitorConfig = configService.getProducerMonitorConfig("song.test");
		System.out.println(producerMonitorConfig);
		if (producerMonitorConfig == null) {
			producerMonitorConfig = configService.newDefaultProducerMonitorConfig("song.test");
		}
		configService.setProducerMonitorConfig(producerMonitorConfig);
		System.out.println(configService.getProducerMonitorConfig("song.test"));
		configService.deleteProducerMonitorConfig("song.test");
	}

}