package com.ctrip.hermes.broker.queue.kafka;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.AbstractMessageQueueDumper;
import com.ctrip.hermes.broker.queue.DefaultMessageQueueDumper;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

public class KafkaMessageQueueDumper extends AbstractMessageQueueDumper {

	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueDumper.class);

	private MessageQueueStorage m_storage;

	public KafkaMessageQueueDumper(String topic, int partition, BrokerConfig config,
	      Lease lease, MessageQueueStorage m_storage, MetaService metaService) {
		super(topic, partition, config, lease);
		this.m_storage = m_storage;
		((KafkaMessageQueueStorage)m_storage).configure(topic, metaService);
	}

	@Override
	protected void doAppendMessageSync(boolean isPriority,
	      Collection<Pair<MessageBatchWithRawData, Map<Integer, Boolean>>> todos) {
		try {
			m_storage.appendMessages(new Tpp(m_topic, m_partition, isPriority), Collections2.transform(todos,
			      new Function<Pair<MessageBatchWithRawData, Map<Integer, Boolean>>, MessageBatchWithRawData>() {

				      @Override
				      public MessageBatchWithRawData apply(Pair<MessageBatchWithRawData, Map<Integer, Boolean>> input) {
					      return input.getKey();
				      }
			      }));

			setBatchesResult(todos, true);
		} catch (Exception e) {
			setBatchesResult(todos, false);
			log.error("Failed to append messages.", e);
		}
	}

	private void setBatchesResult(Collection<Pair<MessageBatchWithRawData, Map<Integer, Boolean>>> todos, boolean success) {
		for (Pair<MessageBatchWithRawData, Map<Integer, Boolean>> todo : todos) {
			bizLog(todo.getKey(), success);
			Map<Integer, Boolean> result = todo.getValue();
			addResults(result, success);
		}
	}

	private void bizLog(MessageBatchWithRawData batch, boolean success) {
		for (PartialDecodedMessage msg : batch.getMessages()) {
			BizEvent event = new BizEvent("Message.Saved");
			event.addData("topic", batch.getTopic());
			event.addData("refKey", msg.getKey());
			event.addData("success", success);

			m_bizLogger.log(event);
		}
	}

}
