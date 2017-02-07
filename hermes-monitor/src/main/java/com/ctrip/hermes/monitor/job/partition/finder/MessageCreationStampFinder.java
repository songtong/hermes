package com.ctrip.hermes.monitor.job.partition.finder;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.admin.core.queue.MessagePriority;
import com.ctrip.hermes.admin.core.queue.MessagePriorityDao;
import com.ctrip.hermes.admin.core.queue.MessagePriorityEntity;
import com.ctrip.hermes.admin.core.queue.TableContext;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.monitor.job.partition.context.MessageTableContext;

@Component
public class MessageCreationStampFinder implements CreationStampFinder {
	private static final Logger log = LoggerFactory.getLogger(MessageCreationStampFinder.class);

	private MessagePriorityDao m_dao = PlexusComponentLocator.lookup(MessagePriorityDao.class);

	@Override
	public CreationStamp findLatest(TableContext ctx) {
		Tpp tpp = ((MessageTableContext) ctx).getTpp();
		try {
			Iterator<MessagePriority> iter = m_dao.latest(tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(),
			      MessagePriorityEntity.READSET_CREATE_DATE).iterator();
			MessagePriority msg = iter.hasNext() ? iter.next() : null;
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find latest message priority failed: {}", ctx, e);
		}
		return null;
	}

	@Override
	public CreationStamp findOldest(TableContext ctx) {
		Tpp tpp = ((MessageTableContext) ctx).getTpp();
		try {
			Iterator<MessagePriority> iter = m_dao.oldest(tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(),
			      MessagePriorityEntity.READSET_CREATE_DATE).iterator();
			MessagePriority msg = iter.hasNext() ? iter.next() : null;
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find oldest message priority failed: {}", ctx, e);
		}
		return null;
	}

	@Override
	public CreationStamp findNearest(TableContext ctx, long id) {
		Tpp tpp = ((MessageTableContext) ctx).getTpp();
		try {
			MessagePriority msg = m_dao.findNearest(tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(), id,
			      MessagePriorityEntity.READSET_CREATE_DATE);
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find nearest id [{}] message priority failed: {}", id, ctx, e);
		}
		return null;
	}
}
