package com.ctrip.hermes.monitor;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.monitor.service.ESMonitorService;

@SpringBootApplication
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = ESMonitorServiceTest.class)
public class ESMonitorServiceTest {
	@Autowired
	private ESMonitorService m_es;

	@Test
	public void testQueryBrokerError() {
		long now = System.currentTimeMillis();
		long before = now - TimeUnit.DAYS.toMillis(90);
		Map<String, Long> errors = m_es.queryBrokerErrorCount(before, now);
		for (Entry<String, Long> entry : errors.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
	}
}
