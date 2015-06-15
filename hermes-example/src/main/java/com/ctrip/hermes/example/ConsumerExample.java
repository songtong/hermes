package com.ctrip.hermes.example;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.example.common.Configuration;
import com.dianping.cat.Cat;

public class ConsumerExample {

	private static Logger logger = LogManager.getLogger(ConsumerExample.class);
	private static String groupId = null;
	private static String topic = null;

	public static void main(String[] args) {
		init();
		runConsumer();
	}

	private static void init() {
		Configuration.addResource("hermes-example.properties");
		topic = Configuration.get("consumer.topic", "cmessage_fws");
		groupId = Configuration.get("consumer.groupid", "group1");
		Cat.initializeByDomain("900777", 2280, 80, "cat.ctripcorp.com");
	}

	private static void runConsumer() {
		logger.info(String.format("Consumer Example Started.\nTopic: %s, GroupId: %s", topic, groupId));

		final AtomicInteger i = new AtomicInteger(0);
		Engine engine = PlexusComponentLocator.lookup(Engine.class);

		Subscriber s = new Subscriber(topic, groupId, new BaseMessageListener<String>(groupId) {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
//				logger.info("==== ConsumedMessage ==== \n" + msg.toString());

				if (i.incrementAndGet() %1000 ==0)
				logger.info("ConsumerReceived count: " + i.get());
			}
		});
		engine.start(Arrays.asList(s));
	}
}
