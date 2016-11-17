package com.ctrip.hermes.monitor.job.partition.finder;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.admin.core.queue.ResendGroupId;
import com.ctrip.hermes.admin.core.queue.ResendGroupIdDao;
import com.ctrip.hermes.admin.core.queue.ResendGroupIdEntity;
import com.ctrip.hermes.admin.core.queue.TableContext;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.monitor.job.partition.context.ResendTableContext;

@Component
public class ResendCreationStampFinder implements CreationStampFinder {

	private static final Logger log = LoggerFactory.getLogger(ResendCreationStampFinder.class);

	private ResendGroupIdDao m_dao = PlexusComponentLocator.lookup(ResendGroupIdDao.class);

	@Override
	public CreationStamp findLatest(TableContext sCtx) {
		ResendTableContext ctx = (ResendTableContext) sCtx;
		try {
			Iterator<ResendGroupId> iter = m_dao.latest(ctx.getTopic().getName(), ctx.getPartition().getId(),
			      ctx.getConsumer().getId(), ResendGroupIdEntity.READSET_CREATE_DATE).iterator();
			ResendGroupId msg = iter.hasNext() ? iter.next() : null;
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find latest resend group failed: {}", ctx, e);
		}
		return null;
	}

	@Override
	public CreationStamp findOldest(TableContext sCtx) {
		ResendTableContext ctx = (ResendTableContext) sCtx;
		try {
			Iterator<ResendGroupId> iter = m_dao.oldest(ctx.getTopic().getName(), ctx.getPartition().getId(),
			      ctx.getConsumer().getId(), ResendGroupIdEntity.READSET_CREATE_DATE).iterator();
			ResendGroupId msg = iter.hasNext() ? iter.next() : null;
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find oldest resend group failed: {}", ctx, e);
		}
		return null;
	}

	@Override
	public CreationStamp findSpecific(TableContext sCtx, long id) {
		ResendTableContext ctx = (ResendTableContext) sCtx;
		try {
			ResendGroupId msg = m_dao.findByPK(id, ctx.getTopic().getName(), ctx.getPartition().getId(), //
			      ctx.getConsumer().getId(), ResendGroupIdEntity.READSET_CREATE_DATE);
			if (msg != null) {
				return new CreationStamp(msg.getId(), msg.getCreationDate());
			}
		} catch (DalException e) {
			log.debug("Find specific id [{}] resend group failed: {}", id, ctx, e);
		}
		return null;
	}
}
