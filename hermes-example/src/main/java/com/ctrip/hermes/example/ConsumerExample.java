package com.ctrip.hermes.example;

import java.util.Arrays;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
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

		Cat.initializeByDomain("900777", 2280, 80, "cat.ctripcorp.com");
	}

	private static void runConsumer() {
		Engine engine = PlexusComponentLocator.lookup(Engine.class);

		Subscriber s = new Subscriber(topic, groupId, new BaseMessageListener<String>(groupId) {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				logger.info("ConsumedMessage: " + msg.toString());
				System.out.println("ConsumerReceived: " + msg.toString());
			}
		});
		engine.start(Arrays.asList(s));
	}
}
