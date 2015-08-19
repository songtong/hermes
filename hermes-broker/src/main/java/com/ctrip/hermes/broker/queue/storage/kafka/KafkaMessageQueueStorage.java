package com.ctrip.hermes.broker.queue.storage.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.meta.entity.Storage;

@Named(type = MessageQueueStorage.class, value = Storage.KAFKA)
public class KafkaMessageQueueStorage implements MessageQueueStorage {

	@Inject
	private MetaService m_metaService;

	@Inject
	private MessageCodec m_messageCodec;

	// TODO housekeeping
	private Map<String, KafkaMessageBrokerSender> m_senders = new HashMap<>();

	@Override
	public void appendMessages(Tpp tpp, Collection<MessageBatchWithRawData> batches) throws Exception {
		ByteBuf buf = Unpooled.buffer();
		KafkaMessageBrokerSender sender = getSender(tpp.getTopic());
		for (MessageBatchWithRawData batch : batches) {
			List<PartialDecodedMessage> pdmsgs = batch.getMessages();
			for (PartialDecodedMessage pdmsg : pdmsgs) {
				m_messageCodec.encodePartial(pdmsg, buf);
				byte[] bytes = new byte[buf.readableBytes()];
				buf.readBytes(bytes);
				buf.clear();
				sender.send(tpp.getTopic(), tpp.getPartition(), bytes);
			}
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
