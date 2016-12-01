package com.ctrip.hermes.collector.rule.eventhandler;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.hub.StateHub;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.collector.state.impl.TPProduceState;
import com.ctrip.hermes.meta.entity.Storage;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class ConsumerLargeBacklogEventHandlerTest {
	@Autowired
	private RulesEngine m_rulesEngine;
	
	@Autowired
	private StateHub m_stateHub;
	
	//@Test
	public void testKafka() throws IOException {
		int count = 9;
		Date current = Calendar.getInstance().getTime();
		while (count >= 0) {
			TPProduceState state = new TPProduceState();
			state.setTopicId(10);
			state.setTopicName("leo_test");
			state.setPartition(count);
			state.setOffsetNonPriority(10000000);
			state.setLastNonPriorityCreationDate(current);
			state.setOffsetPriority(1);
			state.setTimestamp(current.getTime());
			state.setStorageType(Storage.KAFKA);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
		}
		
		count = 9;
		while (count >= 0) {
			TPProduceState state = new TPProduceState();
			state.setTopicId(20);
			state.setTopicName("lxteng_test");
			state.setPartition(count);
			state.setOffsetNonPriority(10000000);
			state.setLastNonPriorityCreationDate(current);
			state.setOffsetPriority(1);
			state.setTimestamp(current.getTime());
			state.setStorageType(Storage.KAFKA);
			state.addObserver(m_rulesEngine);
			if (count == 0) {
				state.setSync(true);
			}
			state.notifyObservers();
			count--;
		}
		
		
		//Thread.sleep(1000);
		count = 0;
		while (count < 10) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(10);
			state.setTopicName("leo_test");
			state.setConsumerGroupId(10);
			state.setConsumerGroup("leo_test_group");
			state.setPartition(count);
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(current);
			state.setOffsetPriority(1);
			state.setTimestamp(current.getTime());
			state.setStorageType(Storage.KAFKA);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count++;
		}
		
		count = 0;
		while (count <= 10) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(20);
			state.setTopicName("lxteng_test");
			state.setConsumerGroupId(20);
			state.setConsumerGroup("lxteng_test_group");
			state.setPartition(count);
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(current);
			state.setOffsetPriority(1);
			state.setTimestamp(current.getTime());
			state.setStorageType(Storage.KAFKA);
			state.addObserver(m_rulesEngine);
			if (count == 10) {
				state.setSync(true);
			}
			state.notifyObservers();
			count++;
		}
		System.in.read();
	}
	
	//@Test
	public void testMysql() throws IOException, InterruptedException {
		int count = 9;
		Date current = Calendar.getInstance().getTime();
		while (count >= 0) {
			TPProduceState state = new TPProduceState();
			state.setTopicId(1);
			state.setTopicName("leo_test");
			state.setPartition(count);
			state.setOffsetNonPriority(10000000);
			state.setLastNonPriorityCreationDate(current);
			state.setOffsetPriority(1);
			state.setTimestamp(current.getTime());
			state.setStorageType(Storage.MYSQL);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
		}
		
		count = 9;
		while (count >= 0) {
			TPProduceState state = new TPProduceState();
			state.setTopicId(2);
			state.setTopicName("lxteng_test");
			state.setPartition(count);
			state.setOffsetNonPriority(10000000);
			state.setLastNonPriorityCreationDate(current);
			state.setOffsetPriority(1);
			state.setTimestamp(current.getTime());
			state.setStorageType(Storage.MYSQL);
			state.addObserver(m_rulesEngine);
			if (count == 0) {
				state.setSync(true);
			}
			state.notifyObservers();
			count--;
		}
		
//		TPProduceState s = new TPProduceState();
//		s.setTopicId(2);
//		s.setTopicName("lxteng_test");
//		s.setPartition(count);
//		s.setOffsetNonPriority(10000000);
//		s.setLastNonPriorityCreationDate(current);
//		s.setOffsetPriority(1);
//		s.setTimestamp(current.getTime());
//		s.setStorageType(Storage.MYSQL);
//		s.addObserver(m_rulesEngine);
//		s.setSync(true);
//		s.notifyObservers();
		
		
		//Thread.sleep(1000);
		count = 0;
		while (count < 10) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(1);
			state.setTopicName("leo_test");
			state.setConsumerGroupId(1);
			state.setConsumerGroup("leo_test_group");
			state.setPartition(count);
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(current);
			state.setOffsetPriority(1);
			state.setTimestamp(current.getTime());
			state.setStorageType(Storage.MYSQL);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count++;
		}
		
		count = 0;
		while (count <= 10) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(2);
			state.setTopicName("lxteng_test");
			state.setConsumerGroupId(2);
			state.setConsumerGroup("lxteng_test_group");
			state.setPartition(count);
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(current);
			state.setOffsetPriority(1);
			state.setTimestamp(current.getTime());
			state.setStorageType(Storage.MYSQL);
			state.addObserver(m_rulesEngine);
			if (count == 10) {
				state.setSync(true);
			}
			state.notifyObservers();
			count++;
		}
		
//		TPGConsumeState state = new TPGConsumeState();
//		state.setTopicId(2);
//		state.setTopicName("lxteng_test");
//		state.setConsumerGroupId(2);
//		state.setConsumerGroup("lxteng_test_group");
//		state.setOffsetNonPriority(1);
//		state.setOffsetPriority(1);
//		state.setSync(true);
//		state.setStorageType(Storage.MYSQL);
//		state.addObserver(m_rulesEngine);
//		state.notifyObservers();
		
		System.in.read();
	}
	
	@Test
	public void testSync() throws IOException {
		long current = System.currentTimeMillis();
		TPGConsumeState state = new TPGConsumeState();
		state.setTimestamp(current);
		state.setStorageType(Storage.MYSQL);
		state.addObserver(m_rulesEngine);
		state.setSync(true);
		//state.notifyObservers();
		m_stateHub.offer(state);
		
		TPProduceState s = new TPProduceState();
		s.setTimestamp(current);
		s.setStorageType(Storage.MYSQL);
		s.addObserver(m_rulesEngine);
		s.setSync(true);
		//s.notifyObservers();
		m_stateHub.offer(s);

		
		//-------------------
		
		s = new TPProduceState();
		s.setTimestamp(current + 10);
		s.setStorageType(Storage.KAFKA);
		s.addObserver(m_rulesEngine);
		s.setSync(true);
		//s.notifyObservers();
		m_stateHub.offer(s);
		
		
		state = new TPGConsumeState();
		state.setTimestamp(current + 10);
		state.setStorageType(Storage.KAFKA);
		state.addObserver(m_rulesEngine);
		state.setSync(true);
		//state.notifyObservers();
		m_stateHub.offer(state);
		
		//----------------
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		state = new TPGConsumeState();
		state.setTimestamp(current + 11);
		state.setStorageType(Storage.KAFKA);
		state.addObserver(m_rulesEngine);
		state.setSync(true);
		//state.notifyObservers();
		m_stateHub.offer(state);
		
		state = new TPGConsumeState();
		state.setTimestamp(current + 13);
		state.setStorageType(Storage.KAFKA);
		state.addObserver(m_rulesEngine);
		state.setSync(true);
		//state.notifyObservers();
		m_stateHub.offer(state);

		
		state = new TPGConsumeState();
		state.setTimestamp(current + 10);
		state.setStorageType(Storage.MYSQL);
		state.addObserver(m_rulesEngine);
		state.setSync(true);
		//state.notifyObservers();
		m_stateHub.offer(state);
		
		s = new TPProduceState();
		s.setTimestamp(current + 13);
		s.setStorageType(Storage.KAFKA);
		s.addObserver(m_rulesEngine);
		s.setSync(true);
		//s.notifyObservers();
		m_stateHub.offer(s);

		
		System.in.read();
	}
}
