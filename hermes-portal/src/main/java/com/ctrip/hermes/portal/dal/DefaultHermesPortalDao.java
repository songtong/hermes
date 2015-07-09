package com.ctrip.hermes.portal.dal;

import java.util.Date;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.portal.config.PortalConstants;

@Named(type = HermesPortalDao.class)
public class DefaultHermesPortalDao implements HermesPortalDao {

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

		MessagePriority msg = null;

		msg = m_msgDao.top(topic, partition, PortalConstants.PRIORITY_TRUE, MessagePriorityEntity.READSET_FULL);
		latestProduced = msg == null ? latestProduced : getNewer(latestProduced, msg.getCreationDate());

		msg = m_msgDao.top(topic, partition, PortalConstants.PRIORITY_FALSE, MessagePriorityEntity.READSET_FULL);
		latestProduced = msg == null ? latestProduced : getNewer(latestProduced, msg.getCreationDate());

		return latestProduced;
	}

	@Override
	public Date getLatestConsumed(String topic, int partition, int group) throws DalException {
		Date latestConsumed = new Date(0);

		OffsetMessage off = null;

		off = m_offsetDao.find(topic, partition, PortalConstants.PRIORITY_TRUE, group, OffsetMessageEntity.READSET_FULL);
		latestConsumed = off == null ? latestConsumed : getNewer(latestConsumed, off.getLastModifiedDate());

		off = m_offsetDao.find(topic, partition, PortalConstants.PRIORITY_FALSE, group, OffsetMessageEntity.READSET_FULL);
		latestConsumed = off == null ? latestConsumed : getNewer(latestConsumed, off.getLastModifiedDate());

		return latestConsumed;
	}
}
