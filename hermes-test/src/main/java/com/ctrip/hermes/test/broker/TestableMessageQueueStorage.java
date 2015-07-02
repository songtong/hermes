package com.ctrip.hermes.test.broker;

import io.netty.buffer.ByteBuf;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class TestableMessageQueueStorage implements MessageQueueStorage {

	private DataPool m_dataPool = new DataPool();

	@Override
	public void appendMessages(Tpp tpp, Collection<MessageBatchWithRawData> batches) throws Exception {
		for (MessageBatchWithRawData batch : batches) {
			m_dataPool.insertMsgs(tpp, batch.getMessages());
		}
	}

	@Override
	public Object findLastOffset(Tpp tpp, int groupId) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object findLastResendOffset(Tpg tpg) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, Object startOffset, int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FetchResult fetchResendMessages(Tpg tpg, Object startOffset, int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, MessageMeta>> msgId2Metas) {
		// TODO Auto-generated method stub

	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {
		// TODO Auto-generated method stub

	}

	public Map<Integer, List<DataRecord>> getMessages(String topic, boolean isPriority) {
		return m_dataPool.getMessages(topic, isPriority);
	}

	public List<DataRecord> getMessages(Tpp tpp) {
		List<DataRecord> msgs = new LinkedList<>();
		List<DataRecord> records = m_dataPool.getMessages(tpp);
		if (records != null) {
			for (DataRecord record : records) {

				msgs.add(record);
			}
		}

		return msgs;
	}

	public DataRecord getMessage(Tpp tpp, String refKey) {
		DataRecord msg = null;
		List<DataRecord> records = m_dataPool.getMessages(tpp);
		if (records != null) {
			for (DataRecord record : records) {
				if (record.getRefKey().equals(refKey)) {
					msg = record;
					break;
				}
			}
		}

		return msg;
	}

	private static class DataPool {
		private ConcurrentMap<Tpp, AtomicLong> m_idGenerators = new ConcurrentHashMap<>();

		private ConcurrentMap<TopicPartition, List<DataRecord>> m_msgs = new ConcurrentHashMap<>();

		private ConcurrentMap<TopicPartition, List<DataRecord>> m_priorityMsgs = new ConcurrentHashMap<>();

		private ConcurrentMap<TopicPartition, List<DataRecord>> m_resends = new ConcurrentHashMap<>();

		private ConcurrentMap<TopicPartition, List<DataRecord>> m_deadLetters = new ConcurrentHashMap<>();

		public Map<Integer, List<DataRecord>> getMessages(String topic, boolean isPriority) {

			Map<Integer, List<DataRecord>> result = new HashMap<>();

			ConcurrentMap<TopicPartition, List<DataRecord>> msgs = isPriority ? m_priorityMsgs : m_msgs;

			for (Map.Entry<TopicPartition, List<DataRecord>> entry : msgs.entrySet()) {
				if (entry.getKey().getTopic().equals(topic)) {
					result.put(entry.getKey().getPartition(), entry.getValue());
				}
			}

			return result;
		}

		public List<DataRecord> getMessages(Tpp tpp) {
			TopicPartition tp = new TopicPartition(tpp.getTopic(), tpp.getPartition());
			if (tpp.isPriority()) {
				return m_priorityMsgs.get(tp);
			} else {
				return m_msgs.get(tp);
			}
		}

		public void insertMsgs(Tpp tpp, List<PartialDecodedMessage> msgs) {
			m_idGenerators.putIfAbsent(tpp, new AtomicLong(0));
			TopicPartition tp = new TopicPartition(tpp.getTopic(), tpp.getPartition());

			ConcurrentMap<TopicPartition, List<DataRecord>> msgTable = null;
			AtomicLong idGenerator = m_idGenerators.get(tpp);
			if (tpp.isPriority()) {
				m_priorityMsgs.putIfAbsent(tp, new LinkedList<DataRecord>());
				msgTable = m_priorityMsgs;
			} else {
				m_msgs.putIfAbsent(tp, new LinkedList<DataRecord>());
				msgTable = m_msgs;
			}

			for (PartialDecodedMessage pdmsg : msgs) {

				DataRecord record = convertToRecord(pdmsg, idGenerator.incrementAndGet());

				msgTable.get(tp).add(record);
			}

		}

		private DataRecord convertToRecord(PartialDecodedMessage pdmsg, long id) {
			DataRecord record = new DataRecord();
			record.setId(id);
			record.setRefKey(pdmsg.getKey());
			record.setResend(false);
			record.setBodyCodecType(pdmsg.getBodyCodecType());
			record.setBody(pdmsg.readBody());
			record.setBornTime(pdmsg.getBornTime());
			record.setRemainingRetries(pdmsg.getRemainingRetries());
			record.setDurableProperties(readProperties(pdmsg.getDurableProperties()));
			record.setVolatileProperties(readProperties(pdmsg.getVolatileProperties()));
			return record;
		}

		private Map<String, String> readProperties(ByteBuf buf) {
			HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
			return codec.readStringStringMap();
		}
	}

	private static class TopicPartition {
		private String m_topic;

		private int m_partition;

		public TopicPartition(String topic, int partition) {
			m_topic = topic;
			m_partition = partition;
		}

		public String getTopic() {
			return m_topic;
		}

		public int getPartition() {
			return m_partition;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + m_partition;
			result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TopicPartition other = (TopicPartition) obj;
			if (m_partition != other.m_partition)
				return false;
			if (m_topic == null) {
				if (other.m_topic != null)
					return false;
			} else if (!m_topic.equals(other.m_topic))
				return false;
			return true;
		}

	}

	public static class DataRecord {
		private long m_id;

		private String m_refKey;

		private boolean m_resend;

		private String m_bodyCodecType;

		private byte[] m_body;

		private long m_bornTime;

		private int m_remainingRetries = 0;

		private Map<String, String> m_durableProperties;

		private Map<String, String> m_volatileProperties;

		public DataRecord() {
		}

		public long getId() {
			return m_id;
		}

		public void setId(long id) {
			m_id = id;
		}

		public String getRefKey() {
			return m_refKey;
		}

		public void setRefKey(String refKey) {
			m_refKey = refKey;
		}

		public boolean isResend() {
			return m_resend;
		}

		public void setResend(boolean resend) {
			m_resend = resend;
		}

		public String getBodyCodecType() {
			return m_bodyCodecType;
		}

		public void setBodyCodecType(String bodyCodecType) {
			m_bodyCodecType = bodyCodecType;
		}

		public byte[] getBody() {
			return m_body;
		}

		public void setBody(byte[] body) {
			m_body = body;
		}

		public long getBornTime() {
			return m_bornTime;
		}

		public void setBornTime(long bornTime) {
			m_bornTime = bornTime;
		}

		public int getRemainingRetries() {
			return m_remainingRetries;
		}

		public void setRemainingRetries(int remainingRetries) {
			m_remainingRetries = remainingRetries;
		}

		public Map<String, String> getDurableProperties() {
			return m_durableProperties;
		}

		public void setDurableProperties(Map<String, String> durableProperties) {
			m_durableProperties = durableProperties;
		}

		public Map<String, String> getVolatileProperties() {
			return m_volatileProperties;
		}

		public void setVolatileProperties(Map<String, String> volatileProperties) {
			m_volatileProperties = volatileProperties;
		}

	}
}
