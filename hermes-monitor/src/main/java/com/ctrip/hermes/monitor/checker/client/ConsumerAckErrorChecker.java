package com.ctrip.hermes.monitor.checker.client;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.admin.core.monitor.event.ConsumerAckErrorEvent;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.monitor.checker.CatBasedChecker;
import com.ctrip.hermes.monitor.checker.CheckerResult;

//@Component(value = ConsumerAckErrorChecker.ID)
public class ConsumerAckErrorChecker extends CatBasedChecker implements InitializingBean {
	private static final Logger log = LoggerFactory.getLogger(ConsumerAckErrorChecker.class);

	public static final String ID = "ConsumerAckErrorChecker";

	private List<String> m_excludeConsumerPatterns;

	@Override
	public String name() {
		return ID;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		String excludeConsumersStr = m_config.getConsumeAckCmdFailRatioExcludeConsumers();
		m_excludeConsumerPatterns = JSON.parseObject(excludeConsumersStr, new TypeReference<List<String>>() {
		});
		log.info("*********  ConsumerAckErrorChecker exclude consumer patterns  ************");
		log.info(m_excludeConsumerPatterns.toString());
	}

	@Override
	protected void doCheck(Timespan timespan, CheckerResult result) throws Exception {
		Map<String, Map<Integer, CatRangeEntity>> catNameRanges = //
		getCatCrossDomainData(timespan, CatConstants.TYPE_MESSAGE_CONSUME_ACK_TRANSPORT);
		System.out.println(catNameRanges);
		for (Entry<String, Map<Integer, CatRangeEntity>> entry : catNameRanges.entrySet()) {
			String[] parts = entry.getKey().split(":");
			if (parts.length >= 2) {
				String topic = parts[0];
				String consumer = parts[1];

				if (isIncludeConsumer(consumer)) {
					Pair<Integer, Integer> pair = sumRanges(timespan, entry.getValue());
					int total = pair.getKey();
					int fails = pair.getValue();
					if (fails / (float) total > m_config.getConsumeAckCmdFailRatioLimit()) {
						result.addMonitorEvent(new ConsumerAckErrorEvent(topic, consumer, total, fails));
					}
				}
			}
		}

		result.setRunSuccess(true);
	}

	private boolean isIncludeConsumer(String consumer) {
		for (String pattern : m_excludeConsumerPatterns) {
			if (Pattern.matches(pattern, consumer)) {
				return false;
			}
		}
		return true;
	}

	private Pair<Integer, Integer> sumRanges(Timespan timespan, Map<Integer, CatRangeEntity> ranges) {
		int total = 0, fails = 0;
		for (Entry<Integer, CatRangeEntity> entry : ranges.entrySet()) {
			if (timespan.getMinutes().contains(entry.getKey())) {
				total += entry.getValue().getCount();
				fails += entry.getValue().getFails();
			}
		}
		return new Pair<Integer, Integer>(total, fails);
	}

}
