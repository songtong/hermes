package com.ctrip.hermes.metaservice.service;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.metaservice.monitor.config.TopicCheckerConfig;
import com.ctrip.hermes.metaservice.monitor.service.CollectorConfigService;

public class DefaultCheckerConfigServiceTest extends ComponentTestCase {

	@Test
	public void test() {
		CollectorConfigService configService = lookup(CollectorConfigService.class);
		TopicCheckerConfig config = configService.getTopicCheckerConfig("song.test");
		System.out.println(config);
		configService.setTopicCheckerConfig(config);
		System.out.println(configService.listConsumeLargeBacklogLimits());
	}

}
