package com.ctrip.hermes.collector.rule.eventhandler;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.collector.state.impl.TPProduceState;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.meta.entity.Storage;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class LongTimeNoConsumeEventHandlerTest {
	@Autowired
	private RulesEngine m_rulesEngine;
	
	@Test
	public void testMysql() throws IOException, InterruptedException {
		int count = 5;
		// Create records 15 mins ago.
		long timestamp = System.currentTimeMillis();
		while (count >= 0) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(1);
			state.setPartitionId(count);
			state.setTopicName("leo_test");
			state.setConsumerGroupId(1);
			state.setConsumerGroup("leo_test_group");
			state.setOffsetNonPriority(10);
			state.setOffsetNonPriorityModifiedDate(new Date(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES)));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.MYSQL);
			state.setTimestamp(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES));
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			
		}
		
		count = 0;
		while (count <= 5) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(1);
			state.setPartitionId(count);
			state.setTopicName("leo_test");
			state.setConsumerGroupId(1);
			state.setConsumerGroup("leo_test_group");
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(new Date(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES)));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.MYSQL);
			state.setTimestamp(System.currentTimeMillis() + 1);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count++;
			
		}
		
		count = 5;
		while (count >= 0) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(2);
			state.setPartitionId(count);
			state.setTopicName("lxteng_test");
			state.setConsumerGroupId(2);
			state.setConsumerGroup("lxteng_test_group");
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(new Date(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES)));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.MYSQL);
			state.setTimestamp(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES));
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			
		}
		
		count = 0;
		while (count <= 5) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(2);
			state.setPartitionId(count);
			state.setTopicName("lxteng_test");
			state.setConsumerGroupId(2);
			state.setConsumerGroup("lxteng_test_group");
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(new Date(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES)));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.MYSQL);
			state.setTimestamp(System.currentTimeMillis() + 1);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count++;
			
		}
		
		
		TPGConsumeState state = new TPGConsumeState();
		state.setStorageType(Storage.MYSQL);
		state.setTimestamp(System.currentTimeMillis());
		state.setSync(true);
		state.addObserver(m_rulesEngine);
		state.notifyObservers();
			
		System.in.read();
	}
	
	//@Test
	public void testKafka() throws IOException, InterruptedException {
		int count = 5;
		// Create records 15 mins ago.
		long timestamp = System.currentTimeMillis();
		while (count >= 0) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(10);
			state.setPartitionId(count);
			state.setTopicName("leo_test");
			state.setConsumerGroupId(10);
			state.setConsumerGroup("leo_test_group");
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(new Date(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES)));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.KAFKA);
			state.setTimestamp(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES));
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			
		}
		
		count = 0;
		while (count <= 5) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(10);
			state.setPartitionId(count);
			state.setTopicName("leo_test");
			state.setConsumerGroupId(10);
			state.setConsumerGroup("leo_test_group");
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(new Date(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES)));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.KAFKA);
			state.setTimestamp(System.currentTimeMillis() + 1);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count++;
			
		}
		
		count = 5;
		while (count >= 0) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(20);
			state.setPartitionId(count);
			state.setTopicName("lxteng_test");
			state.setConsumerGroupId(20);
			state.setConsumerGroup("lxteng_test_group");
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(new Date(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES)));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.KAFKA);
			state.setTimestamp(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES));
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count--;
			
		}
		
		count = 0;
		while (count <= 5) {
			TPGConsumeState state = new TPGConsumeState();
			state.setTopicId(20);
			state.setPartitionId(count);
			state.setTopicName("lxteng_test");
			state.setConsumerGroupId(20);
			state.setConsumerGroup("lxteng_test_group");
			state.setOffsetNonPriority(1);
			state.setOffsetNonPriorityModifiedDate(new Date(TimeUtils.before(timestamp, 15, TimeUnit.MINUTES)));
			state.setOffsetPriority(1);
			state.setStorageType(Storage.KAFKA);
			state.setTimestamp(System.currentTimeMillis() + 1);
			state.addObserver(m_rulesEngine);
			state.notifyObservers();
			count++;
			
		}
		
		
		TPGConsumeState state = new TPGConsumeState();
		state.setStorageType(Storage.KAFKA);
		state.setTimestamp(System.currentTimeMillis());
		state.setSync(true);
		state.addObserver(m_rulesEngine);
		state.notifyObservers();
			
		System.in.read();
	}
}
