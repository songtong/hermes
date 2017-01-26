package com.ctrip.hermes.monitor.job.partition.finder;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.admin.core.queue.DeadLetter;
import com.ctrip.hermes.admin.core.queue.DeadLetterDao;
import com.ctrip.hermes.admin.core.queue.DeadLetterEntity;
import com.ctrip.hermes.admin.core.queue.TableContext;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.job.partition.context.DeadLetterTableContext;

@Component
public class DeadLetterCreationStampFinder implements CreationStampFinder {
	private static final Logger log = LoggerFactory.getLogger(DeadLetterCreationStampFinder.class);

	private DeadLetterDao m_dao = PlexusComponentLocator.lookup(DeadLetterDao.class);

	@Override
	public CreationStamp findLatest(TableContext ctx) {
		Topic topic = ((DeadLetterTableContext) ctx).getTopic();
		Partition partition = ((DeadLetterTableContext) ctx).getPartition();
		try {
			Iterator<DeadLetter> iter = m_dao.latest( //
			      topic.getName(), partition.getId(), DeadLetterEntity.READSET_CREATE_DATE).iterator();
			DeadLetter msg = iter.hasNext() ? iter.next() : null;
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find latest dead letter failed: {}", ctx, e);
		}
		return null;
	}

	@Override
	public CreationStamp findOldest(TableContext ctx) {
		Topic topic = ((DeadLetterTableContext) ctx).getTopic();
		Partition partition = ((DeadLetterTableContext) ctx).getPartition();
		try {
			Iterator<DeadLetter> iter = m_dao.oldest(topic.getName(), partition.getId(),
			      DeadLetterEntity.READSET_CREATE_DATE).iterator();
			DeadLetter msg = iter.hasNext() ? iter.next() : null;
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find oldest dead letter failed: {}", ctx, e);
		}
		return null;
	}

	@Override
	public CreationStamp findNearest(TableContext ctx, long id) {
		Topic topic = ((DeadLetterTableContext) ctx).getTopic();
		Partition partition = ((DeadLetterTableContext) ctx).getPartition();
		try {
			DeadLetter msg = m_dao.findNearest(topic.getName(), partition.getId(), id,
			      DeadLetterEntity.READSET_CREATE_DATE);
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find nearest id [{}] dead letter failed: {}", id, ctx, e);
		}
		return null;
	}
}
