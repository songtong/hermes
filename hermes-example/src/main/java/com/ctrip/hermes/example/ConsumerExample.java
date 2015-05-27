package com.ctrip.hermes.example;

import java.util.Arrays;

import com.ctrip.framework.clogging.agent.config.LogConfig;
import com.ctrip.framework.clogging.agent.log.ILog;
import com.ctrip.framework.clogging.agent.log.LogManager;
import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.common.Configuration;
import com.dianping.cat.Cat;

public class ConsumerExample {

	private static ILog logger = LogManager.getLogger(ConsumerExample.class);
	private static String groupId = null;
	private static String topic = null;

	public static void main(String[] args) {
		init();
		runConsumer();
	}

	private static void init() {
		Configuration.addResource("consumer-example.properties");

		groupId = Configuration.get("consumer.groupid", "group1");
		topic = Configuration.get("consumer.topic", "order_new");

		LogConfig.setAppID(Configuration.get("hermes.rest.appid", "900777"));
		LogConfig.setLoggingServerIP(Configuration.get("clog.collector.ip", "collector.logging.sh.ctriptravel.com"));
		LogConfig.setLoggingServerPort(Configuration.get("clog.collector.port", "63100"));

		Cat.initializeByDomain("900777", 2280, 80, Configuration.get("cat.url", "cat.ctripcorp.com"));
	}

	private static void runConsumer() {
		Engine engine = PlexusComponentLocator.lookup(Engine.class);

		Subscriber s = new Subscriber(topic, groupId, new BaseMessageListener<String>(groupId) {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				logger.info("ConsumedMessage", msg.toString());
				System.out.println("ConsumerReceived: " + msg.toString());
			}
		});
		engine.start(Arrays.asList(s));
	}
}
