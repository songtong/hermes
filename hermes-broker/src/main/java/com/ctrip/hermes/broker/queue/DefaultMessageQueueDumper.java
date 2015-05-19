package com.ctrip.hermes.broker.queue;

import java.util.Collection;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageQueueDumper extends AbstractMessageQueueDumper {

	private MessageQueueStorage m_storage;

	public DefaultMessageQueueDumper(String topic, int partition, MessageQueueStorage storage,
	      SystemClockService systemClockSerivce, BrokerConfig config, Lease lease) {
		super(topic, partition, systemClockSerivce, config, lease);
		m_storage = storage;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void setBatchesResult(Collection<Pair<MessageBatchWithRawData, Map<Integer, Boolean>>> todos, boolean success) {
		for (Pair<MessageBatchWithRawData, Map<Integer, Boolean>> todo : todos) {
			Map<Integer, Boolean> result = todo.getValue();
			addResults(result, success);
		}
	}

}
