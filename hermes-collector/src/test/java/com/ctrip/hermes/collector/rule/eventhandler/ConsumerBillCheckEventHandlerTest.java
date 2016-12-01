package com.ctrip.hermes.collector.rule.eventhandler;

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
import com.ctrip.hermes.meta.entity.Storage;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CollectorTest.class)
public class ConsumerBillCheckEventHandlerTest {
	@Autowired
	private RulesEngine m_rulesEngine;

	@Test
	public void test() throws Exception {
		Date current = Calendar.getInstance().getTime();
		for (int i = 0; i < 3; i++) {
			for (int count = 0; count < 10; count++) {
				TPGConsumeState state = new TPGConsumeState();
				state.setTopicId(1);
				state.setTopicName("sys.song.test");
				state.setConsumerGroupId(123);
				state.setConsumerGroup("sys.song.test");
				state.setPartition(count);
				state.setOffsetNonPriority((i + 1) * count);
				state.setOffsetNonPriorityModifiedDate(current);
				state.setOffsetPriority((i + 1) * count * 2);
				state.setTimestamp(System.currentTimeMillis());
				state.setStorageType(Storage.KAFKA);
				state.addObserver(m_rulesEngine);
				if (count == 10) {
					state.setSync(true);
				}
				state.notifyObservers();
				System.out.println("*************** ONE EVENT");
			}
			TimeUnit.SECONDS.sleep(5);
		}
		System.in.read();
	}
}
