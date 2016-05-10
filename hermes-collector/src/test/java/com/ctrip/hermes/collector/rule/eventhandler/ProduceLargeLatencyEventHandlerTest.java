package com.ctrip.hermes.collector.rule.eventhandler;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import scala.util.Random;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.impl.ProduceLatencyState;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class ProduceLargeLatencyEventHandlerTest {
	@Autowired
	private RulesEngine m_rulesEngine;
	
	@Test
	public void testHandler() throws IOException {
		//Random random = new Random();
		for (int index = 0; index < 5; index++) {
			ProduceLatencyState state = new ProduceLatencyState("leo_test", 100, 1, 1, 1);
			state.setCountAll(1000);
			state.setRatio((double)state.getCount() / state.getCountAll());
			state.setTimestamp(System.currentTimeMillis());
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
		}
		
		for (int index = 0; index < 5; index++) {
			ProduceLatencyState state = new ProduceLatencyState("lxteng_test", 200, 1, 1, 1);
			state.setCountAll(1000);
			state.setRatio((double)state.getCount() / state.getCountAll());
			state.setTimestamp(System.currentTimeMillis());
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
		}
		
		System.in.read();
		
	}
}
