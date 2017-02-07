package com.ctrip.hermes.monitor.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@Configuration
@ComponentScan(basePackages = "com.ctrip.hermes.monitor.config")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = PartitionCheckerConfigTest.class)
public class PartitionCheckerConfigTest {
	@Autowired
	PartitionCheckerConfig m_config;

	@Test
	public void test() {
		m_config.printPartitionConfigs();
	}

}
