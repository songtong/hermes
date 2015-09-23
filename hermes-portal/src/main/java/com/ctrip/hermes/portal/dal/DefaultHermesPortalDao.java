package com.ctrip.hermes.portal.dal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.portal.assist.ListUtils;
import com.ctrip.hermes.portal.config.PortalConstants;

@Named(type = HermesPortalDao.class)
public class DefaultHermesPortalDao implements HermesPortalDao {
	private static final Logger log = LoggerFactory.getLogger(DefaultHermesPortalDao.class);

	@Inject
	private MessagePriorityDao m_msgDao;

	@Inject
	private OffsetMessageDao m_offsetDao;

	@Override
	public MessagePriority getLatestProduced(String topic, int partition, int priority) throws DalException {
		MessagePriority msg = doFindLatestMessage(topic, partition, priority);
		return msg;
	}

	@Override
	public MessagePriority getMsgById(String topic, int partition, int priority, long id) throws DalException {
		if(id<=0){
			return null;
		}
		List<MessagePriority> msgs = m_msgDao.findIdAfter(topic, partition, priority, id - 1, 1,
				MessagePriorityEntity.READSET_FULL);
		return msgs.size() > 0 ? msgs.get(0) : null;
	}

	@Override
	public Map<Integer, Pair<OffsetMessage, OffsetMessage>> getLatestConsumed(String topic, int partition)
			throws DalException {
		List<OffsetMessage> offsetMsgs = m_offsetDao.findAll(topic, partition, OffsetMessageEntity.READSET_FULL);
		Map<Integer, Pair<OffsetMessage, OffsetMessage>> offsetMsgMap = new HashMap<>();
		for (OffsetMessage offsetMsg : offsetMsgs) {

			if (!offsetMsgMap.containsKey(offsetMsg.getGroupId()))
				offsetMsgMap.put(offsetMsg.getGroupId(), new Pair<OffsetMessage, OffsetMessage>());

			if (PortalConstants.PRIORITY_TRUE == offsetMsg.getPriority()) {
				offsetMsgMap.get(offsetMsg.getGroupId()).setKey(offsetMsg);
			} else {
				offsetMsgMap.get(offsetMsg.getGroupId()).setValue(offsetMsg);
			}

		}
		return offsetMsgMap;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<MessagePriority> getLatestMessages(String topic, int partition, int count) throws DalException {
		List<MessagePriority> k0 = doFindLatestMessages(topic, partition, PortalConstants.PRIORITY_TRUE, count);
		List<MessagePriority> k1 = doFindLatestMessages(topic, partition, PortalConstants.PRIORITY_FALSE, count);
		return ListUtils.getTopK(count, new Comparator<MessagePriority>() {
			@Override
			public int compare(MessagePriority o1, MessagePriority o2) {
				return o1.getCreationDate().compareTo(o2.getCreationDate());
			}
		}, new List[] { k0, k1 });
	}

	private MessagePriority doFindLatestMessage(String topic, int partition, int priority) {
		List<MessagePriority> list = doFindLatestMessages(topic, partition, priority, 1);
		return list.size() > 0 ? list.get(0) : null;
	}

	private List<MessagePriority> doFindLatestMessages(String topic, int partition, int priority, int count) {
		try {
			return m_msgDao.topK(topic, partition, priority, count, MessagePriorityEntity.READSET_FULL);
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Find top K failed: {} {}", topic, partition, e);
			}
			return new ArrayList<MessagePriority>();
		}
	}

}
