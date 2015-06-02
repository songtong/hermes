package com.ctrip.hermes.rest.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.payload.RawMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.rest.resource.TopicsResource;
import com.google.common.io.ByteStreams;

@Named
public class ProducerService {

	@Inject
	private MetaService metaService;

	private final Producer producer = Producer.getInstance();

	public boolean topicExist(String topicName) {
		Topic topic = metaService.findTopicByName(topicName);
		return topic != null;
	}

	public Future<SendResult> send(String topic, Map<String, String> params, InputStream is)
	      throws MessageSendException, IOException {
		byte[] payload = ByteStreams.toByteArray(is);
		RawMessage rawMsg = new RawMessage(payload);

		String partitionKey = null;
		if (params.containsKey(TopicsResource.PARTITION_KEY)) {
			partitionKey = params.get(TopicsResource.PARTITION_KEY);
		}
		MessageHolder messageHolder = producer.message(topic, partitionKey, rawMsg);
		if (params.containsKey(TopicsResource.PRIORITY)) {
			messageHolder.withPriority();
		}
		if (params.containsKey(TopicsResource.REF_KEY)) {
			String refKey = params.get(TopicsResource.REF_KEY);
			messageHolder.withRefKey(refKey);
		}
		if (params.containsKey(TopicsResource.PROPERTIES)) {
			String properties = params.get(TopicsResource.PROPERTIES);
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
