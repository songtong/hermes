package com.ctrip.hermes.kafka.engine;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.bootstrap.BaseConsumerBootstrap;
import com.ctrip.hermes.consumer.engine.bootstrap.ConsumerBootstrap;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.transport.command.CorrelationIdGenerator;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.kafka.message.KafkaConsumerMessage;
import com.ctrip.hermes.kafka.util.KafkaProperties;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = ConsumerBootstrap.class, value = Endpoint.KAFKA)
public class KafkaConsumerBootstrap extends BaseConsumerBootstrap {

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaConsumerBootstrap.class);

	private ExecutorService m_executor = Executors.newCachedThreadPool(HermesThreadFactory.create(
	      "KafkaConsumerExecutor", true));

	@Inject
	private ClientEnvironment m_environment;

	@Inject
	private MessageCodec m_messageCodec;

	private Map<ConsumerContext, KafkaConsumerThread> consumers = new HashMap<ConsumerContext, KafkaConsumerThread>();

	private Map<ConsumerContext, Long> tokens = new HashMap<ConsumerContext, Long>();

	@Override
	protected SubscribeHandle doStart(final ConsumerContext consumerContext) {
		Topic topic = consumerContext.getTopic();

		Properties prop = getConsumerProperties(topic.getName(), consumerContext.getGroupId());

		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(prop);

		long token = CorrelationIdGenerator.generateCorrelationId();
		m_consumerNotifier.register(token, consumerContext);
		KafkaConsumerThread consumerThread = new KafkaConsumerThread(consumer, consumerContext, token);
		m_executor.submit(consumerThread);

		consumers.put(consumerContext, consumerThread);
		tokens.put(consumerContext, token);

		return new SubscribeHandle() {

			@Override
			public void close() {
				doStop(consumerContext);
			}
		};
	}

	@Override
	protected void doStop(ConsumerContext consumerContext) {
		KafkaConsumerThread consumerThread = consumers.remove(consumerContext);
		consumerThread.shutdown();

		Long token = tokens.remove(consumerContext);
		m_consumerNotifier.deregister(token);

		super.doStop(consumerContext);
	}

	class KafkaConsumerThread implements Runnable {

		private final AtomicBoolean closed = new AtomicBoolean(false);

		private KafkaConsumer<String, byte[]> consumer;

		private ConsumerContext consumerContext;

		private long token;

		public KafkaConsumerThread(KafkaConsumer<String, byte[]> consumer, ConsumerContext consumerContext,
		      long token) {
			this.consumer = consumer;
			this.consumerContext = consumerContext;
			this.token = token;
		}

		@Override
		public void run() {
			try {
				consumer.subscribe(Arrays.asList(consumerContext.getTopic().getName()));
				Set<TopicPartition> assignment = consumer.assignment();
				m_logger.info("Current assignment: " + assignment);
				while (!closed.get()) {
					ConsumerRecords<String, byte[]> records = consumer.poll(5000);
					for (ConsumerRecord<String, byte[]> consumerRecord : records) {
						long offset = -1;
						try {
							offset = consumerRecord.offset();
							ByteBuf byteBuf = Unpooled.wrappedBuffer(consumerRecord.value());

							BaseConsumerMessage<?> baseMsg = m_messageCodec.decode(consumerContext.getTopic().getName(),
							      byteBuf, consumerContext.getMessageClazz());
							@SuppressWarnings("rawtypes")
							ConsumerMessage kafkaMsg = new KafkaConsumerMessage(baseMsg, consumerRecord.partition(),
							      consumerRecord.offset());
							List<ConsumerMessage<?>> msgs = new ArrayList<ConsumerMessage<?>>(1);
							msgs.add(kafkaMsg);
							m_consumerNotifier.messageReceived(token, msgs);
						} catch (Exception e) {
							m_logger.warn(
							      "Kafka consumer failed Topic:{} Partition:{} Offset:{} Group:{} SesssionId:{} Exception:{}",
							      consumerRecord.topic(), consumerRecord.partition(), offset, consumerContext.getGroupId(),
							      consumerContext.getSessionId(), e.getMessage());
						}
					}
				}
			} catch (WakeupException e) {
				if (!closed.get())
					throw e;
			} finally {
				Set<TopicPartition> assignment = consumer.assignment();
				consumer.commitSync();
				consumer.close();
				m_logger.info("Close assignment: " + assignment);		
			}
		}

		public void shutdown() {
			Set<TopicPartition> assignment = consumer.assignment();
			closed.set(true);
			consumer.wakeup();
			m_logger.info("Shutting down assignment: " + assignment);			
		}
	}

	private Properties getConsumerProperties(String topic, String group) {
		Properties configs = KafkaProperties.getDefaultKafkaConsumerProperties();

		try {
			Properties envProperties = m_environment.getConsumerConfig(topic);
			configs.putAll(envProperties);
		} catch (IOException e) {
			m_logger.warn("kafka read consumer config failed", e);
		}

		List<Partition> partitions = m_metaService.listPartitionsByTopic(topic);
		if (partitions == null || partitions.size() < 1) {
			return configs;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorageByTopic(topic);
		if (targetStorage == null) {
			return configs;
		}

		for (Datasource datasource : targetStorage.getDatasources()) {
			if (consumerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					configs.put(prop.getValue().getName(), prop.getValue().getValue());
				}
				break;
			}
		}
		configs.put("group.id", group);
		return KafkaProperties.overrideByCtripDefaultConsumerSetting(configs, topic, group);
	}
}
