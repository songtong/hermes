package com.ctrip.hermes.collector.rule.eventhandler;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.meta.entity.Storage;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class DeadLetterEventHandlerTest {
	@Autowired
	private RulesEngine m_rulesEngine;
	
	@Test
	public void testHandler() throws IOException {
		int count = 0;
		
		while (true) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(1);
			state.setConsumerGroupId(1);
			state.setTopicName("leo_test");
			state.setConsumerGroup("leo_test_group");
			state.setPartitionId(count);
			state.setDeadLetterCount(2);
			state.setTimestamp(System.currentTimeMillis());
			state.setStorageType(Storage.MYSQL);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			if (++count == 10) {
	            break;
			}
		} 
		
		TPGConsumeState state = new TPGConsumeState();
        state.setTopicId(1);
        state.setConsumerGroupId(1);
        state.setTopicName("leo_test");
        state.setConsumerGroup("leo_test_group");
        state.setPartitionId(1);
        state.setDeadLetterCount(10);
        state.setTimestamp(System.currentTimeMillis());
		state.setStorageType(Storage.MYSQL);
        state.addObserver(m_rulesEngine);
        state.notifyObservers();
		
		state = new TPGConsumeState();
		state.setStorageType(Storage.MYSQL);
        state.setSync(true);
        state.setTimestamp(System.currentTimeMillis());
        state.addObserver(m_rulesEngine);
        state.notifyObservers();
		
		System.in.read();
	}
}
