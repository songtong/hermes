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
import com.ctrip.hermes.collector.state.impl.CommandDropState;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState.ErrorSample;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class CommandDropEventHandlerTest {
	@Autowired
	private RulesEngine m_rulesEngine;
	
	@Test
	public void testHandler() throws IOException {
		int count = 3;
		while (count > 0) {
		    CommandDropState state = new CommandDropState("Send.Cmd.V1", (short)1, 1);
		    state.setHost("host");
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
