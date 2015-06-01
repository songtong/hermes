package com.ctrip.hermes.rest.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.payload.RawMessage;
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

	public Future<SendResult> send(String topic, Map<String, String> params, InputStream is)
	      throws MessageSendException, IOException {
		byte[] payload = ByteStreams.toByteArray(is);
		RawMessage rawMsg = new RawMessage(payload);
		MessageHolder messageHolder = producer.message(topic, params.get("partitionKey"), rawMsg);
		Future<SendResult> sendResult = messageHolder.send();
		return sendResult;
	}

}
