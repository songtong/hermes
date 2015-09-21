package com.ctrip.hermes.broker.queue.storage.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.dal.hermes.DeadLetter;
import com.ctrip.hermes.broker.dal.hermes.DeadLetterDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetResend;
import com.ctrip.hermes.broker.dal.hermes.OffsetResendDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetResendEntity;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupId;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdDao;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdEntity;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.BizLogger;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.TransferCallback;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.meta.entity.Storage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MessageQueueStorage.class, value = Storage.MYSQL)
public class MySQLMessageQueueStorage implements MessageQueueStorage {
	private static final Logger log = LoggerFactory.getLogger(MySQLMessageQueueStorage.class);

	@Inject
	private BizLogger m_bizLogger;

	@Inject
	private MessageCodec m_messageCodec;

	@Inject
	private MessagePriorityDao m_msgDao;

	@Inject
	private ResendGroupIdDao m_resendDao;

	@Inject
	private OffsetResendDao m_offsetResendDao;

	@Inject
	private OffsetMessageDao m_offsetMessageDao;

	@Inject
	private DeadLetterDao m_deadLetterDao;

	@Inject
	private MetaService m_metaService;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private BrokerConfig m_config;

	private Map<Triple<String, Integer, Integer>, OffsetResend> m_offsetResendCache = new ConcurrentHashMap<>();

	private Map<Pair<Tpp, Integer>, OffsetMessage> m_offsetMessageCache = new ConcurrentHashMap<>();

	@Override
	public void appendMessages(Tpp tpp, Collection<MessageBatchWithRawData> batches) throws Exception {
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

				if (msgs.size() == m_config.getMySQLBatchInsertSize()) {
					batchInsert(tpp, msgs);
					msgs.clear();
				}
			}
		}

		if (!msgs.isEmpty()) {
			batchInsert(tpp, msgs);
		}
	}

	private void batchInsert(Tpp tpp, List<MessagePriority> msgs) throws DalException {
		long startTime = m_systemClockService.now();
		m_msgDao.insert(msgs.toArray(new MessagePriority[msgs.size()]));

		bizLog(tpp, msgs, startTime, m_systemClockService.now());
	}

	private void bizLog(Tpp tpp, List<MessagePriority> msgs, long startTime, long endTime) {
		BizEvent mysqlEvent = new BizEvent("MySQL.Insert");
		mysqlEvent.addData("startTime", startTime);
		mysqlEvent.addData("elapse", endTime - startTime);
		mysqlEvent.addData("topic", tpp.getTopic());
		mysqlEvent.addData("partition", tpp.getPartition());
		mysqlEvent.addData("priority", tpp.getPriorityInt());
		mysqlEvent.addData("msgCount", msgs.size());
		m_bizLogger.log(mysqlEvent);

		for (MessagePriority msg : msgs) {
			BizEvent event = new BizEvent("RefKey.Transformed");
			event.addData("refKey", msg.getRefKey());
			event.addData("msgId", msg.getId());

			m_bizLogger.log(event);
		}
	}

	@Override
	public synchronized Object findLastOffset(Tpp tpp, int groupId) throws Exception {
		String topic = tpp.getTopic();
		int partition = tpp.getPartition();
		int priority = tpp.getPriorityInt();

		List<OffsetMessage> lastOffset = m_offsetMessageDao.find(topic, partition, priority, groupId,
		      OffsetMessageEntity.READSET_FULL);

		if (lastOffset.isEmpty()) {
			List<MessagePriority> topMsg = m_msgDao.top(topic, partition, priority, MessagePriorityEntity.READSET_FULL);

			long startOffset = 0L;
			if (!topMsg.isEmpty()) {
				startOffset = CollectionUtil.last(topMsg).getId();
			}

			OffsetMessage offset = new OffsetMessage();
			offset.setCreationDate(new Date());
			offset.setGroupId(groupId);
			offset.setOffset(startOffset);
			offset.setPartition(partition);
			offset.setPriority(priority);
			offset.setTopic(topic);

			m_offsetMessageDao.insert(offset);
			return offset.getOffset();
		} else {
			return CollectionUtil.last(lastOffset).getOffset();
		}
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, Object startOffset, int batchSize) {
		FetchResult result = new FetchResult();
		try {
			final List<MessagePriority> dataObjs = m_msgDao.findIdAfter(tpp.getTopic(), tpp.getPartition(),
			      tpp.getPriorityInt(), (Long) startOffset, batchSize, MessagePriorityEntity.READSET_FULL);

			long biggestOffset = 0L;
			if (dataObjs != null && !dataObjs.isEmpty()) {
				final TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
				for (MessagePriority dataObj : dataObjs) {
					MessageMeta msgMeta = new MessageMeta(dataObj.getId(), 0, dataObj.getId(), tpp.getPriorityInt(), false);
					biggestOffset = Math.max(biggestOffset, dataObj.getId());
					batch.addMessageMeta(msgMeta);
				}
				final String topic = tpp.getTopic();
				batch.setTopic(topic);
				batch.setPartition(tpp.getPartition());
				batch.setResend(false);
				batch.setPriority(tpp.getPriorityInt());

				batch.setTransferCallback(new TransferCallback() {

					@Override
					public void transfer(ByteBuf out) {
						for (MessagePriority dataObj : dataObjs) {
							PartialDecodedMessage partialMsg = new PartialDecodedMessage();
							partialMsg.setRemainingRetries(0);
							partialMsg.setDurableProperties(Unpooled.wrappedBuffer(dataObj.getAttributes()));
							partialMsg.setBody(Unpooled.wrappedBuffer(dataObj.getPayload()));
							partialMsg.setBornTime(dataObj.getCreationDate().getTime());
							partialMsg.setKey(dataObj.getRefKey());
							partialMsg.setBodyCodecType(dataObj.getCodecType());

							m_messageCodec.encodePartial(partialMsg, out);
						}
					}

				});

				result.setBatch(batch);
				result.setOffset(biggestOffset);
				return result;
			}
		} catch (Exception e) {
			log.error("Failed to fetch message(topic={}, partition={}, priority={}).", tpp.getTopic(), tpp.getPartition(),
			      tpp.isPriority(), e);
		}

		return null;
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, MessageMeta>> msgId2Metas) {
		if (CollectionUtil.isNotEmpty(msgId2Metas)) {
			try {

				RetryPolicy retryPolicy = m_metaService.findRetryPolicyByTopicAndGroup(tpp.getTopic(), groupId);

				List<Pair<Long, MessageMeta>> toDeadLetter = new ArrayList<>();
				List<Pair<Long, MessageMeta>> toResend = new ArrayList<>();
				for (Pair<Long, MessageMeta> pair : msgId2Metas) {
					MessageMeta meta = pair.getValue();
					if (resend) {
						meta.setRemainingRetries(meta.getRemainingRetries() - 1);
					} else {
						meta.setRemainingRetries(retryPolicy.getRetryTimes());
					}

					if (meta.getRemainingRetries() <= 0) {
						toDeadLetter.add(pair);
					} else {
						toResend.add(pair);
					}

				}

				copyToDeadLetter(tpp, groupId, toDeadLetter, resend);
				copyToResend(tpp, groupId, toResend, resend, retryPolicy);
			} catch (Exception e) {
				log.error("Failed to nack messages(topic={}, partition={}, priority={}, groupId={}).", tpp.getTopic(),
				      tpp.getPartition(), tpp.isPriority(), groupId, e);
			}
		}
	}

	private void copyToResend(Tpp tpp, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas, boolean resend,
	      RetryPolicy retryPolicy) throws DalException {
		if (CollectionUtil.isNotEmpty(msgId2Metas)) {
			long now = m_systemClockService.now();

			if (!resend) {
				ResendGroupId proto = new ResendGroupId();
				proto.setTopic(tpp.getTopic());
				proto.setPartition(tpp.getPartition());
				proto.setPriority(tpp.getPriorityInt());
				proto.setGroupId(m_metaService.translateToIntGroupId(tpp.getTopic(), groupId));
				proto.setScheduleDate(new Date(retryPolicy.nextScheduleTimeMillis(0, now)));
				proto.setMessageIds(collectOffset(msgId2Metas));
				proto.setRemainingRetries(retryPolicy.getRetryTimes());

				m_resendDao.copyFromMessageTable(proto);
			} else {
				List<ResendGroupId> protos = new LinkedList<>();
				for (Pair<Long, MessageMeta> pair : msgId2Metas) {
					ResendGroupId proto = new ResendGroupId();
					proto.setTopic(tpp.getTopic());
					proto.setPartition(tpp.getPartition());
					proto.setPriority(tpp.getPriorityInt());
					proto.setGroupId(m_metaService.translateToIntGroupId(tpp.getTopic(), groupId));
					int retryTimes = retryPolicy.getRetryTimes() - pair.getValue().getRemainingRetries();
					proto.setScheduleDate(new Date(retryPolicy.nextScheduleTimeMillis(retryTimes, now)));
					proto.setId(pair.getKey());

					protos.add(proto);

				}
				m_resendDao.copyFromResendTable(protos.toArray(new ResendGroupId[protos.size()]));
			}

		}
	}

	private void copyToDeadLetter(Tpp tpp, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas, boolean resend)
	      throws DalException {
		if (CollectionUtil.isNotEmpty(msgId2Metas)) {
			DeadLetter proto = new DeadLetter();
			proto.setTopic(tpp.getTopic());
			proto.setPartition(tpp.getPartition());
			proto.setPriority(tpp.getPriorityInt());
			proto.setGroupId(m_metaService.translateToIntGroupId(tpp.getTopic(), groupId));
			proto.setDeadDate(new Date());
			proto.setMessageIds(collectOffset(msgId2Metas));

			if (resend) {
				m_deadLetterDao.copyFromResendTable(proto);
			} else {
				m_deadLetterDao.copyFromMessageTable(proto);
			}
		}
	}

	private Long[] collectOffset(List<Pair<Long, MessageMeta>> msgId2Metas) {
		Long[] offsets = new Long[msgId2Metas.size()];

		int idx = 0;
		for (Pair<Long, MessageMeta> pair : msgId2Metas) {
			offsets[idx++] = pair.getKey();
		}

		return offsets;
	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {
		try {
			String topic = tpp.getTopic();
			int partition = tpp.getPartition();
			int intGroupId = m_metaService.translateToIntGroupId(tpp.getTopic(), groupId);
			if (resend) {
				OffsetResend proto = getOffsetResend(topic, partition, intGroupId);

				ResendGroupId resendRow = m_resendDao.findByPK(msgSeq, topic, partition, intGroupId,
				      ResendGroupIdEntity.READSET_FULL);

				proto.setTopic(topic);
				proto.setPartition(partition);
				proto.setLastScheduleDate(resendRow.getScheduleDate());
				proto.setLastId(resendRow.getId());

				m_offsetResendDao.increaseOffset(proto, OffsetResendEntity.UPDATESET_OFFSET);
			} else {
				OffsetMessage proto = getOffsetMessage(tpp, intGroupId);
				proto.setTopic(topic);
				proto.setPartition(partition);
				proto.setOffset(msgSeq);

				m_offsetMessageDao.increaseOffset(proto, OffsetMessageEntity.UPDATESET_OFFSET);
			}
		} catch (DalException e) {
			log.error("Failed to ack messages(topic={}, partition={}, priority={}, groupId={}).", tpp.getTopic(),
			      tpp.getPartition(), tpp.isPriority(), groupId, e);
		}
	}

	private OffsetMessage getOffsetMessage(Tpp tpp, int intGroupId) throws DalException {
		Pair<Tpp, Integer> key = new Pair<>(tpp, intGroupId);

		if (!m_offsetMessageCache.containsKey(key)) {
			synchronized (m_offsetMessageCache) {
				if (!m_offsetMessageCache.containsKey(key)) {
					List<OffsetMessage> offsetMessageRow = m_offsetMessageDao.find(tpp.getTopic(), tpp.getPartition(),
					      tpp.getPriorityInt(), intGroupId, OffsetMessageEntity.READSET_FULL);

					OffsetMessage proto = CollectionUtil.first(offsetMessageRow);
					m_offsetMessageCache.put(key, proto);
				}
			}
		}
		return m_offsetMessageCache.get(key);
	}

	private OffsetResend getOffsetResend(String topic, int partition, int intGroupId) throws DalException {
		Triple<String, Integer, Integer> tpg = new Triple<String, Integer, Integer>(topic, partition, intGroupId);

		if (!m_offsetResendCache.containsKey(tpg)) {
			synchronized (m_offsetResendCache) {
				if (!m_offsetResendCache.containsKey(tpg)) {
					List<OffsetResend> offsetResendRow = m_offsetResendDao.top(tpg.getFirst(), tpg.getMiddle(),
					      tpg.getLast(), OffsetResendEntity.READSET_FULL);

					OffsetResend proto = CollectionUtil.first(offsetResendRow);
					m_offsetResendCache.put(tpg, proto);
				}
			}
		}
		return m_offsetResendCache.get(tpg);
	}

	@Override
	public synchronized Object findLastResendOffset(Tpg tpg) throws Exception {
		int groupId = m_metaService.translateToIntGroupId(tpg.getTopic(), tpg.getGroupId());
		List<OffsetResend> tops = m_offsetResendDao.top(tpg.getTopic(), tpg.getPartition(), groupId,
		      OffsetResendEntity.READSET_FULL);
		if (CollectionUtil.isNotEmpty(tops)) {
			OffsetResend top = CollectionUtil.first(tops);
			return new Pair<>(top.getLastScheduleDate(), top.getLastId());
		} else {
			OffsetResend proto = new OffsetResend();
			proto.setTopic(tpg.getTopic());
			proto.setPartition(tpg.getPartition());
			proto.setGroupId(m_metaService.translateToIntGroupId(tpg.getTopic(), tpg.getGroupId()));
			proto.setLastScheduleDate(new Date(0));
			proto.setLastId(0L);
			proto.setCreationDate(new Date());

			m_offsetResendDao.insert(proto);
			return new Pair<>(proto.getLastScheduleDate(), proto.getLastId());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public FetchResult fetchResendMessages(Tpg tpg, Object startOffset, int batchSize) {
		Pair<Date, Long> startPair = (Pair<Date, Long>) startOffset;
		FetchResult result = new FetchResult();

		try {
			final List<ResendGroupId> dataObjs = m_resendDao.find(tpg.getTopic(), tpg.getPartition(),
			      m_metaService.translateToIntGroupId(tpg.getTopic(), tpg.getGroupId()), startPair.getKey(), batchSize,
			      startPair.getValue(), new Date(), ResendGroupIdEntity.READSET_FULL);

			if (CollectionUtil.isNotEmpty(dataObjs)) {
				TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
				ResendGroupId latestResend = new ResendGroupId();
				latestResend.setScheduleDate(new Date(0));
				latestResend.setId(0L);

				for (ResendGroupId dataObj : dataObjs) {
					if (resendAfter(dataObj, latestResend)) {
						latestResend = dataObj;
					}
					// TODO origin id, priority
					MessageMeta msgMeta = new MessageMeta(dataObj.getId(), dataObj.getRemainingRetries(), -1,
					      dataObj.getPriority(), true);

					batch.addMessageMeta(msgMeta);

				}
				final String topic = tpg.getTopic();
				batch.setTopic(topic);
				batch.setPartition(tpg.getPartition());
				batch.setResend(true);

				batch.setTransferCallback(new TransferCallback() {

					@Override
					public void transfer(ByteBuf out) {
						for (ResendGroupId dataObj : dataObjs) {
							PartialDecodedMessage partialMsg = new PartialDecodedMessage();
							partialMsg.setRemainingRetries(dataObj.getRemainingRetries());
							partialMsg.setDurableProperties(Unpooled.wrappedBuffer(dataObj.getAttributes()));
							partialMsg.setBody(Unpooled.wrappedBuffer(dataObj.getPayload()));
							partialMsg.setBornTime(dataObj.getCreationDate().getTime());
							partialMsg.setKey(dataObj.getRefKey());
							partialMsg.setBodyCodecType(dataObj.getCodecType());

							m_messageCodec.encodePartial(partialMsg, out);
						}
					}

				});

				result.setBatch(batch);
				result.setOffset(new Pair<Date, Long>(latestResend.getScheduleDate(), latestResend.getId()));
			}
			return result;
		} catch (DalException e) {
			log.error("Failed to fetch resend messages(topic={}, partition={}, groupId={}).", tpg.getTopic(),
			      tpg.getPartition(), tpg.getGroupId(), e);
		}

		return null;
	}

	private boolean resendAfter(ResendGroupId l, ResendGroupId r) {
		if (l.getScheduleDate().after(r.getScheduleDate())) {
			return true;
		}
		if (l.getScheduleDate().equals(r.getScheduleDate()) && l.getId() > r.getId()) {
			return true;
		}

		return false;
	}

}
