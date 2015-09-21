package com.ctrip.hermes.rest.service;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Future;

import com.ctrip.hermes.core.message.payload.RawMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.google.common.io.ByteStreams;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;

public class ProducerSendCommand extends HystrixCommand<Future<SendResult>> {

	private Producer producer;

	private String topic;

	private Map<String, String> params;

	private InputStream is;

	public ProducerSendCommand(Producer producer, String topic, Map<String, String> params, InputStream is) {
		super(HystrixCommandGroupKey.Factory.asKey(ProducerSendCommand.class.getSimpleName()));
		this.producer = producer;
		this.topic = topic;
		this.params = params;
		this.is = is;
	}

	@Override
	protected Future<SendResult> run() throws Exception {
		byte[] payload = ByteStreams.toByteArray(is);
		RawMessage rawMsg = new RawMessage(payload);

		String partitionKey = null;
		if (params.containsKey("partitionKey")) {
			partitionKey = params.get("partitionKey");
		}
		MessageHolder messageHolder = producer.message(topic, partitionKey, rawMsg);
		if (params.containsKey("priority")) {
			if (Boolean.valueOf(params.get("priority"))) {
				messageHolder.withPriority();
			}
		}
		if (params.containsKey("refKey")) {
			String refKey = params.get("refKey");
			messageHolder.withRefKey(refKey);
		}
		if (params.containsKey("properties")) {
			String properties = params.get("properties");
			for (String pro : properties.split(",")) {
				if (pro.contains("=")) {
					String[] split = pro.split("=");
					if (split.length == 2) {
						messageHolder.addProperty(split[0], split[1]);
					}
				}
			}
		}

		Future<SendResult> sendResult = messageHolder.send();
		return sendResult;
	}

}
