package com.ctrip.hermes.collector.rule.eventhandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState.ErrorSample;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState.SourceType;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class ServiceErrorEventHandlerTest {
	@Autowired
	private RulesEngine m_rulesEngine;
	
	@Test
	public void testHandler() throws IOException {
		int count = 3;
		while (count > 0) {
			ServiceErrorState state = ServiceErrorState.newServiceErrorState(SourceType.BROKER);
			state.setCount(1);
			Map<String, Long> counts = new HashMap<String, Long>();
			counts.put("host1", 1L);
			counts.put("host2", 1L);
			state.setCountOnHosts(counts);
			ErrorSample sample = new ErrorSample();
			sample.setTimestamp(System.currentTimeMillis());
			sample.setHostname("host1");
			sample.setMessage("message1");
			state.addErrorOnHost("host1", sample);
			sample = new ErrorSample();
			sample.setTimestamp(System.currentTimeMillis());
			sample.setHostname("host2");
			sample.setMessage("message2");
			state.addErrorOnHost("host2", sample);
			state.setTimestamp(System.currentTimeMillis());
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.in.read();
		
	}
}
