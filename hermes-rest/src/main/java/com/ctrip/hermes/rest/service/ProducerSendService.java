package com.ctrip.hermes.rest.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.producer.api.Producer;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;

@Named
public class ProducerSendService {

	@Inject
	private MetaService metaService;

	private final Producer producer = Producer.getInstance();

	public boolean topicExist(String topicName) {
		Topic topic = metaService.findTopicByName(topicName);
		return topic != null;
	}

	public Future<SendResult> send(String topic, Map<String, String> params, InputStream is)
	      throws MessageSendException, IOException {
		HystrixRequestContext context = HystrixRequestContext.initializeContext();
		try {
			return new ProducerSendCommand(producer, topic, params, is).execute();
		} finally {
			context.shutdown();
		}
	}
}
