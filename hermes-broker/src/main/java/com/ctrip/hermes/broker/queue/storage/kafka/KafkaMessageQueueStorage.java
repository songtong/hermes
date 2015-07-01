package com.ctrip.hermes.broker.queue.storage.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.BizLogger;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.meta.entity.Storage;

@Named(type = MessageQueueStorage.class, value = Storage.KAFKA)
public class KafkaMessageQueueStorage implements MessageQueueStorage {

	@Inject
	private BizLogger m_bizLogger;

	@Inject
	private MetaService m_metaService;

	// TODO housekeeping
	private Map<String, KafkaMessageBrokerSender> m_senders = new HashMap<>();

	@Override
	public void appendMessages(Tpp tpp, Collection<MessageBatchWithRawData> batches) throws Exception {

		KafkaMessageBrokerSender sender = getSender(tpp.getTopic());

		List<MessagePriority> msgs = new ArrayList<>();
		for (MessageBatchWithRawData batch : batches) {
			List<PartialDecodedMessage> pdmsgs = batch.getMessages();
			for (PartialDecodedMessage pdmsg : pdmsgs) {
				MessagePriority msg = new MessagePriority();
				msg.setAttributes(pdmsg.readDurableProperties());
				msg.setCreationDate(new Date(pdmsg.getBornTime()));
				msg.setPartition(tpp.getPartition());
				msg.setPayload(pdmsg.readBody());
				msg.setPriority(tpp.isPriority() ? 0 : 1);
				// TODO set producer id and producer id in producer
				msg.setProducerId(0);
				msg.setProducerIp("");
				msg.setRefKey(pdmsg.getKey());
				msg.setTopic(tpp.getTopic());
				msg.setCodecType(pdmsg.getBodyCodecType());

				msgs.add(msg);
				sender.send(msg);
			}
		}

		bizLog(msgs);
	}

	private void bizLog(List<MessagePriority> msgs) {
		for (MessagePriority msg : msgs) {
			BizEvent event = new BizEvent("RefKey.Transformed");
			event.addData("refKey", msg.getRefKey());
			event.addData("msgId", msg.getId());

			m_bizLogger.log(event);
		}
	}

	@Override
	public Object findLastOffset(Tpp tpp, int groupId) throws Exception {
		return null;
	}

	@Override
	public Object findLastResendOffset(Tpg tpg) throws Exception {
		return null;
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, Object startOffset, int batchSize) {
		return null;
	}

	@Override
	public FetchResult fetchResendMessages(Tpg tpg, Object startOffset, int batchSize) {
		return null;
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, MessageMeta>> msgId2Metas) {

	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {

	}

	private KafkaMessageBrokerSender getSender(String topic) {
		if (!m_senders.containsKey(topic)) {
			synchronized (m_senders) {
				if (!m_senders.containsKey(topic)) {
					m_senders.put(topic, new KafkaMessageBrokerSender(topic, m_metaService));
				}
			}
		}

		return m_senders.get(topic);
	}

}
