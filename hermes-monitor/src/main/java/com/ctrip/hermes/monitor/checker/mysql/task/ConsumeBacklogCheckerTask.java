package com.ctrip.hermes.monitor.checker.mysql.task;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.metaservice.queue.MessagePriority;
import com.ctrip.hermes.metaservice.queue.MessagePriorityDao;
import com.ctrip.hermes.metaservice.queue.MessagePriorityEntity;
import com.ctrip.hermes.metaservice.queue.OffsetMessage;
import com.ctrip.hermes.metaservice.queue.OffsetMessageDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessageEntity;
import com.ctrip.hermes.monitor.checker.CheckerResult;

import io.netty.util.internal.ConcurrentSet;

public class ConsumeBacklogCheckerTask implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(ConsumeBacklogCheckerTask.class);

	private Map<Pair<Topic, ConsumerGroup>, Long> m_limits;

	private MessagePriorityDao m_msgDao;

	private OffsetMessageDao m_offsetDao;

	private CheckerResult m_result;

	private CountDownLatch m_latch;

	private ConcurrentSet<Exception> m_exceptions;

	public ConsumeBacklogCheckerTask(Map<Pair<Topic, ConsumerGroup>, Long> limits, MessagePriorityDao msgDao,
	      OffsetMessageDao offsetDao, CheckerResult result, CountDownLatch latch, ConcurrentSet<Exception> exceptions) {
		m_limits = limits;
		m_msgDao = msgDao;
		m_offsetDao = offsetDao;
		m_result = result;
		m_latch = latch;
		m_exceptions = exceptions;
	}

	private Map<Integer, Long> calculateBacklog(Topic topic, ConsumerGroup group) {
		Map<Integer, Long> backlogs = new HashMap<Integer, Long>();

		for (Partition partition : topic.getPartitions()) {
			long pBacklog = 0;
			if (topic.isPriorityMessageEnabled()) {
				pBacklog = doCalculateBacklog(topic.getName(), partition.getId(), 0, group.getId());
			}
			long npBacklog = doCalculateBacklog(topic.getName(), partition.getId(), 1, group.getId());
			backlogs.put(partition.getId(), pBacklog + npBacklog);
		}

		return backlogs;
	}

	private long doCalculateBacklog(String topic, int partition, int priority, int group) {
		try {
			Iterator<MessagePriority> latestIter = //
			m_msgDao.latest(topic, partition, priority, MessagePriorityEntity.READSET_ID).iterator();
			long latestMsgId = latestIter.hasNext() ? latestIter.next().getId() : -1;

			List<OffsetMessage> offset = null;
			try {
				offset = m_offsetDao.find(topic, partition, priority, group, OffsetMessageEntity.READSET_FULL);
			} catch (DalException e) {
				log.debug("Find offset message failed.{} {} {} {}", topic, partition, priority, group, e);
			}
			if (!CollectionUtil.isNullOrEmpty(offset)) {
				return offset.get(0) == null ? 0L : Math.max(latestMsgId - offset.get(0).getOffset(), 0);
			}
			return 0L;
		} catch (Exception e) {
			log.debug("Query latest consume backlog failed: {} {} {} {}", topic, partition, priority, group, e);
			m_exceptions.add(e);
			return 0L;
		}
	}

	@Override
	public void run() {
		try {
			for (Entry<Pair<Topic, ConsumerGroup>, Long> entry : m_limits.entrySet()) {
				Topic topic = entry.getKey().getKey();
				ConsumerGroup group = entry.getKey().getValue();
				long limit = entry.getValue();

				Map<Integer, Long> backlogs = calculateBacklog(topic, group);

				long totalBacklog = 0;
				for (Long backlog : backlogs.values()) {
					totalBacklog += backlog;
				}

				if (totalBacklog >= limit) {
					ConsumeLargeBacklogEvent e = new ConsumeLargeBacklogEvent(topic.getName(), group.getName(), backlogs);
					e.setCreateTime(new Date());
					m_result.addMonitorEvent(e);
				}
			}
		} catch (Exception e) {
			m_exceptions.add(e);
		} finally {
			m_latch.countDown();
		}
	}
}
