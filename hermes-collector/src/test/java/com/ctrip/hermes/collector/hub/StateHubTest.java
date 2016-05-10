package com.ctrip.hermes.collector.hub;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.State;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class StateHubTest {
	@Autowired
	private StateHub m_statehub;
	
	@Autowired
	private RulesEngine m_engine;
	
	@Test
	public void testStateHub() {
		int num = 10;
		while (num > 0) {
			m_statehub.offer(new TestState(num));
			num--;
		}
	}
	
	
	public class TestState extends State {
		public TestState(Object id) {
			super(id);
		}
		
		@Override
		protected void doUpdate(State state) {
		}

		@Override
		protected Object generateId() {
			return null;
		}
		
	}
}
