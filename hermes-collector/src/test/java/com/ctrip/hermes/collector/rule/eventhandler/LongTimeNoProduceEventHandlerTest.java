package com.ctrip.hermes.collector.rule.eventhandler;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.impl.TPProduceState;
import com.ctrip.hermes.meta.entity.Storage;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class LongTimeNoProduceEventHandlerTest {
	@Autowired
	private RulesEngine m_rulesEngine;
	
	@Test
	public void testMysql() throws IOException, InterruptedException {
		int count = 5;
		while (count > 0) {
			TPProduceState state = new TPProduceState();
			state.setTopicId(1);
			state.setTopicName("leo_test");
			state.setPartitionId(count);
			state.setOffsetNonPriority(1);
			if (count % 2 == 0) {
				state.setLastNonPriorityCreationDate(new Date(System.currentTimeMillis() - 10 * 60 * 1000 - count * 1000));
			}
			state.setOffsetPriority(1);
			state.setStorageType(Storage.MYSQL);
			state.setTimestamp(System.currentTimeMillis());
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			
		}
		
		count = 5;
		while (count > 0) {
			TPProduceState state = new TPProduceState();
			state.setTopicId(2);
			state.setTopicName("lxteng_test");
			state.setPartitionId(count);
			state.setOffsetNonPriority(1);
			if (count % 2 == 0) {
				state.setLastNonPriorityCreationDate(new Date(System.currentTimeMillis() - 10 * 60 * 1000 + count * 1000));
			}
			state.setOffsetPriority(1);
			state.setTimestamp(System.currentTimeMillis());
			state.setStorageType(Storage.MYSQL);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			
		}
		
		//Thread.sleep(3000);
		
		TPProduceState state = new TPProduceState();
		state.setTopicId(1);
		state.setTopicName("leo_test");
		state.setOffsetNonPriority(1);
		state.setLastNonPriorityCreationDate(new Date());
		state.setOffsetPriority(1);
		state.setStorageType(Storage.MYSQL);
		state.setSync(true);
		state.setTimestamp(System.currentTimeMillis());
		state.addObserver(m_rulesEngine);
		state.notifyObservers();
		System.in.read();
	}
	
	//@Test
	public void testKafka() throws IOException, InterruptedException {
		int count = 5;
		while (count > 0) {
			TPProduceState state = new TPProduceState();
			state.setTopicId(3);
			state.setTopicName("leo_test");
			state.setPartitionId(count);
			state.setOffsetNonPriority(1);
			state.setLastNonPriorityCreationDate(new Date(System.currentTimeMillis() - 11 * 60 * 1000));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.KAFKA);
			state.setTimestamp(System.currentTimeMillis() - 11 * 60 * 1000 - count * 1000);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			
		}
		
		count = 5;
		while (count > 0) {
			TPProduceState state = new TPProduceState();
			state.setTopicId(3);
			state.setTopicName("leo_test");
			state.setPartitionId(count);
			state.setOffsetNonPriority(1);
			state.setLastNonPriorityCreationDate(new Date());
			state.setOffsetPriority(1);
			state.setTimestamp(System.currentTimeMillis() - count * 1000);
			state.setStorageType(Storage.KAFKA);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			
		}
		
		//Thread.sleep(3000);
		
		TPProduceState state = new TPProduceState();
		state.setTopicId(3);
		state.setTopicName("leo_test");
		state.setOffsetNonPriority(1);
		state.setOffsetPriority(1);
		state.setStorageType(Storage.KAFKA);
		state.setSync(true);
		state.setTimestamp(System.currentTimeMillis());
		state.addObserver(m_rulesEngine);
		state.notifyObservers();
			
		System.in.read();
	}
}
