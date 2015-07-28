package com.ctrip.hermes.portal.dal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

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

	private Date getNewer(Date d1, Date d2) {
		return d1.after(d2) ? d1 : d2;
	}

	@Override
	public Pair<Date, Date> getDelayTime(String topic, int partition, int groupId) throws DalException {
		return new Pair<Date, Date>(getLatestProduced(topic, partition), getLatestConsumed(topic, partition, groupId));
	}

	@Override
	public Date getLatestProduced(String topic, int partition) throws DalException {
		Date latestProduced = new Date(0);

		MessagePriority msg = doFindLatestMessage(topic, partition, PortalConstants.PRIORITY_TRUE);
		latestProduced = msg == null ? latestProduced : getNewer(latestProduced, msg.getCreationDate());

		msg = doFindLatestMessage(topic, partition, PortalConstants.PRIORITY_FALSE);
		latestProduced = msg == null ? latestProduced : getNewer(latestProduced, msg.getCreationDate());

		return latestProduced;
	}

	@Override
	public Date getLatestConsumed(String topic, int partition, int group) throws DalException {
		Date latestConsumed = new Date(0);

		OffsetMessage off = findOffsetMessage(topic, partition, PortalConstants.PRIORITY_TRUE, group);
		latestConsumed = off == null ? latestConsumed : getNewer(latestConsumed, off.getLastModifiedDate());

		off = findOffsetMessage(topic, partition, PortalConstants.PRIORITY_FALSE, group);
		latestConsumed = off == null ? latestConsumed : getNewer(latestConsumed, off.getLastModifiedDate());

		return latestConsumed;
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

	private OffsetMessage findOffsetMessage(String topic, int partition, int priority, int groupId) {
		try {
			return m_offsetDao.find(topic, partition, priority, groupId, OffsetMessageEntity.READSET_FULL);
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Find offset message failed, topic:{} priority:{} group:{}", topic, partition, groupId, e);
			}
			return null;
		}
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
