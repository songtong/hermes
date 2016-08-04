package com.ctrip.hermes.rest.service;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Future;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.ctrip.hermes.core.message.payload.AvroPayloadCodec;
import com.ctrip.hermes.core.message.payload.RawMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.rest.service.json.CharSequenceDeserializer;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;

import hermes.ubt.custom.ServerCustomEvent;

public class ProducerSendCommand extends HystrixCommand<Future<SendResult>> {
	private static final ParserConfig parserConfig = new ParserConfig();

	static {
		parserConfig.putDeserializer(CharSequence.class, CharSequenceDeserializer.instance);
	}

	private Producer producer;

	private String topic;

	private Map<String, String> params;

	private InputStream is;

	public ProducerSendCommand(Producer producer, String topic, Map<String, String> params, InputStream is) {
		super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(ProducerSendCommand.class.getSimpleName()))
		      .andCommandKey(HystrixCommandKey.Factory.asKey(topic))
		      .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(5000)));
		this.producer = producer;
		this.topic = topic;
		this.params = params;
		this.is = is;
	}

	@Override
	protected Future<SendResult> run() throws Exception {
		byte[] payload = ByteStreams.toByteArray(is);

		String transform = params.get("transform");
		if ("true".equalsIgnoreCase(transform)) {
			if ("ubt.servercustom.created".equals(topic)) {
				payload = PlexusComponentLocator.lookup(AvroPayloadCodec.class).encode(
				      topic,
				      JSON.parseObject(new String(payload, Charsets.UTF_8), ServerCustomEvent.class, parserConfig,
				            JSON.DEFAULT_PARSER_FEATURE));
			} else {
				throw new IllegalArgumentException(String.format("Message transform not support for topic %s", topic));
			}
		}

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
