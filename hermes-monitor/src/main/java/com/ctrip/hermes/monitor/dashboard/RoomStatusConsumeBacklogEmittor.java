package com.ctrip.hermes.monitor.dashboard;

import java.util.Date;
import java.util.Iterator;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.queue.MessagePriority;
import com.ctrip.hermes.metaservice.queue.MessagePriorityDao;
import com.ctrip.hermes.metaservice.queue.MessagePriorityEntity;
import com.ctrip.hermes.metaservice.queue.OffsetMessage;
import com.ctrip.hermes.metaservice.queue.OffsetMessageDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessageEntity;
import com.ctrip.hermes.monitor.service.ESMonitorService;

@Component
public class RoomStatusConsumeBacklogEmittor {

	private static final Logger log = LoggerFactory.getLogger(DashboardSuite.class);

	private MessagePriorityDao m_msgDao;

	private OffsetMessageDao m_offsetDao;

	@Autowired
	private ESMonitorService esMonitorService;

	@PostConstruct
	public void afterPropertiesSet() throws Exception {
		m_msgDao = PlexusComponentLocator.lookup(MessagePriorityDao.class);
		m_offsetDao = PlexusComponentLocator.lookup(OffsetMessageDao.class);
	}

	private String topic = "hotel.product.roomstatuschange";

	private int group = 18; // hotel.product.roomstatuschange.roomstatusrelation

	private int partitionCount = 10; // temporary checker, hard code

	private int priority = 1; // non-priority only is enough

	public void emit() {
		try {
			for (int i = 0; i < partitionCount; i++) {
				long partitonBacklog = calculateBacklog(topic, i, priority, group);
				DashboardItem item = makeBizItem(i, partitonBacklog);
				esMonitorService.prepareIndex(item);
			}
		} catch (Exception e) {
			log.error("Error check room status consume backlog", e);
		}

	}

	private DashboardItem makeBizItem(int partition, long partitonBacklog) {
		DashboardItem item = new DashboardItem();

		item.setCategory("ConsumeBacklog");
		item.setGroup(group);
		item.setPartition(partition);
		item.setPriority(priority);
		item.setTopic(topic);
		item.setSamplingTimestamp(System.currentTimeMillis());
		item.setTimestamp(new Date());

		item.addValue("delay", partitonBacklog);

		return item;
	}

	private long calculateBacklog(String topic, int partition, int priority, int group) {
		try {
			Iterator<MessagePriority> latestIter = //
			m_msgDao.topK(topic, partition, priority, 1, MessagePriorityEntity.READSET_ID).iterator();
			long latestMsgId = latestIter.hasNext() ? latestIter.next().getId() : -1;

			OffsetMessage offset = null;
			try {
				offset = m_offsetDao.find(topic, partition, priority, group, OffsetMessageEntity.READSET_FULL);
			} catch (DalException e) {
				if (log.isDebugEnabled()) {
					log.debug("Find offset message failed.{} {} {} {}", topic, partition, priority, group, e);
				}
			}
			long consumeOffset = offset == null ? 0 : offset.getOffset();

			return Math.max(latestMsgId - consumeOffset, 0);
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Query latest consume backlog failed: {} {} {} {}", topic, partition, priority, group, e);
			}
			return 0;
		}
	}

}
