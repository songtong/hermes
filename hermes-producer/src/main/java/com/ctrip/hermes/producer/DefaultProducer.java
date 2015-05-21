package com.ctrip.hermes.producer;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.result.Callback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.build.BuildConstants;

@Named(type = Producer.class)
public class DefaultProducer extends Producer {
	@Inject(BuildConstants.PRODUCER)
	private Pipeline<Future<SendResult>> m_pipeline;

	@Inject
	private SystemClockService m_systemClockService;

	@Override
	public DefaultMessageHolder message(String topic, String partitionKey, Object body) {
		return new DefaultMessageHolder(topic, partitionKey, body);
	}

	class DefaultMessageHolder implements MessageHolder {
		private ProducerMessage<Object> m_msg;

		public DefaultMessageHolder(String topic, String partitionKey, Object body) {
			m_msg = new ProducerMessage<Object>(topic, body);
			m_msg.setPartitionKey(partitionKey);
		}

		@Override
		public Future<SendResult> send() {
			m_msg.setBornTime(m_systemClockService.now());
			return m_pipeline.put(m_msg);
		}

		@Override
		public MessageHolder withRefKey(String key) {
			m_msg.setKey(key);
			return this;
		}

		@Override
		public MessageHolder withPriority() {
			m_msg.setPriority(true);
			return this;
		}

		@Override
		public MessageHolder addProperty(String key, String value) {
			m_msg.addDurableAppProperty(key, value);
			return this;
		}

		@Override
		public MessageHolder setCallback(Callback callback) {
			m_msg.setCallback(callback);
			return this;
		}
	}
}
