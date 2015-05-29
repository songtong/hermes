package com.ctrip.hermes.rest.service;

import java.util.Map;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

@Named
public class ProducerService {

	private final Producer producer = Producer.getInstance();

	public boolean topicExist(String topicName) {
		return true;
	}

	public Future<SendResult> send(String topic, Map<String, String> params, Object content) throws MessageSendException {
		MessageHolder messageHolder = producer.message(topic, params.get("partitionKey"), content);
		Future<SendResult> sendResult = messageHolder.send();
		return sendResult;
	}

}
