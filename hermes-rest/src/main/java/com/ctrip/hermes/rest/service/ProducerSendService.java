package com.ctrip.hermes.rest.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Future;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.service.SubscriptionService;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.producer.api.Producer;
import qunar.tc.qmq.producer.MessageProducerProvider;

@Named
public class ProducerSendService implements Initializable{
	private static Logger logger = LoggerFactory.getLogger(ProducerSendService.class);

	@Inject
	private MetaService metaService;

	@Inject
	private SubscriptionService m_subscriptionService;

	private final Producer producer = Producer.getInstance();

	private final MessageProducerProvider provider = new MessageProducerProvider();

	public boolean topicExist(String topicName) {
		Topic topic = metaService.findTopicByName(topicName);
		return topic != null;
	}

	public boolean isQmqTopicExist(String topicName) {
		try {
			return null != m_subscriptionService.findSubscriptionByTypeAndTopic(SubscriptionService.QMQ_TYPE, topicName);
		} catch (DalException e) {
			return false;
		}
	}

	public Future<?> send(String topic, Map<String, String> params, InputStream is, boolean isQmq)
	      throws MessageSendException, IOException {
		if (isQmq) {
			return new QmqProducerSendCommand(provider, topic, params, is).execute();
		} else {
			return new ProducerSendCommand(producer, topic, params, is).execute();
		}
	}

	public Producer getProducer() {
		return this.producer;
	}

	public void initialize() {
		try {
			provider.afterPropertiesSet();
		} catch (Exception e) {
			logger.error("Failed to initialize qmq producer provider.", e);
		}
	}
}
