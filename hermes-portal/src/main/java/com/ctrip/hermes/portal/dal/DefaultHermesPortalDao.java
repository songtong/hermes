package com.ctrip.hermes.portal.dal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
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

		try {
			MessagePriority msg = null;

			msg = m_msgDao.top(topic, partition, PortalConstants.PRIORITY_TRUE, MessagePriorityEntity.READSET_FULL);
			latestProduced = msg == null ? latestProduced : getNewer(latestProduced, msg.getCreationDate());

			msg = m_msgDao.top(topic, partition, PortalConstants.PRIORITY_FALSE, MessagePriorityEntity.READSET_FULL);
			latestProduced = msg == null ? latestProduced : getNewer(latestProduced, msg.getCreationDate());
		} catch (DalNotFoundException e) {
			if (log.isDebugEnabled()) {
				log.debug("Table has no records: {} {}", topic, partition);
			}
		}

		return latestProduced;
	}

	@Override
	public Date getLatestConsumed(String topic, int partition, int group) throws DalException {
		Date latestConsumed = new Date(0);

		try {
			OffsetMessage off = null;

			off = m_offsetDao.find(topic, partition, PortalConstants.PRIORITY_TRUE, group,
			      OffsetMessageEntity.READSET_FULL);
			latestConsumed = off == null ? latestConsumed : getNewer(latestConsumed, off.getLastModifiedDate());

			off = m_offsetDao.find(topic, partition, PortalConstants.PRIORITY_FALSE, group,
			      OffsetMessageEntity.READSET_FULL);
			latestConsumed = off == null ? latestConsumed : getNewer(latestConsumed, off.getLastModifiedDate());
		} catch (DalNotFoundException e) {
			if (log.isDebugEnabled()) {
				log.debug("Table has no records: {} {}", topic, partition);
			}
		}

		return latestConsumed;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<MessagePriority> getLatestMessages(String topic, int partition, int count) throws DalException {
		try {
			List<MessagePriority> k0 = m_msgDao.topK(topic, partition, PortalConstants.PRIORITY_TRUE, count,
			      MessagePriorityEntity.READSET_FULL);
			List<MessagePriority> k1 = m_msgDao.topK(topic, partition, PortalConstants.PRIORITY_FALSE, count,
			      MessagePriorityEntity.READSET_FULL);
			return ListUtils.getTopK(count, new Comparator<MessagePriority>() {
				@Override
				public int compare(MessagePriority o1, MessagePriority o2) {
					return o1.getCreationDate().compareTo(o2.getCreationDate());
				}
			}, new List[] { k0, k1 });
		} catch (DalNotFoundException e) {
			if (log.isDebugEnabled()) {
				log.debug("Table has no records: {} {}", topic, partition);
			}
		}
		return new ArrayList<MessagePriority>();
	}
}
