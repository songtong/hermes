package com.ctrip.hermes.rest.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.google.common.io.ByteStreams;

@Named
public class ProducerService {

	private final Producer producer = Producer.getInstance();

	public boolean topicExist(String topicName) {
		return true;
	}

	public Future<SendResult> send(String topic, Map<String, String> params, Object content)
	      throws MessageSendException, IOException {
		if (content instanceof InputStream) {
			InputStream is = (InputStream) content;
			content = ByteStreams.toByteArray(is);
		}
		MessageHolder messageHolder = producer.message(topic, params.get("partitionKey"), content);
		Future<SendResult> sendResult = messageHolder.send();
		return sendResult;
	}

}
