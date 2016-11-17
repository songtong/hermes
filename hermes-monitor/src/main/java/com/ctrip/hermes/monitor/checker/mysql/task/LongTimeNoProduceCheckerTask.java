package com.ctrip.hermes.monitor.checker.mysql.task;

import io.netty.util.internal.ConcurrentSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.admin.core.monitor.event.LongTimeNoProduceEvent;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.admin.core.queue.MessagePriority;
import com.ctrip.hermes.admin.core.queue.MessagePriorityDao;
import com.ctrip.hermes.admin.core.queue.MessagePriorityEntity;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.checker.CheckerResult;

public class LongTimeNoProduceCheckerTask implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(LongTimeNoProduceCheckerTask.class);

	private MessagePriorityDao m_dao;

	private CountDownLatch m_latch;

	private Topic m_topic;

	private Map<Integer, Integer> m_limitsInMin;

	private CheckerResult m_result;

	private ConcurrentSet<Exception> m_exceptions;

	public LongTimeNoProduceCheckerTask(Topic topic, Map<Integer, Integer> limitInMin, MessagePriorityDao dao,
	      CheckerResult result, CountDownLatch latch, ConcurrentSet<Exception> exceptions) {
		m_latch = latch;
		m_dao = dao;
		m_topic = topic;
		m_limitsInMin = limitInMin;
		m_result = result;
		m_exceptions = exceptions;
	}

	private CreationStamp queryLatestProduceCreationStamp(int partition, int priority) throws Exception {
		Iterator<MessagePriority> iter = m_dao.latest( //
		      m_topic.getName(), partition, priority, MessagePriorityEntity.READSET_CREATE_DATE).iterator();
		MessagePriority msg = iter.hasNext() ? iter.next() : null;
		return msg == null ? null : new CreationStamp(msg.getId(), msg.getCreationDate());
	}

	private CreationStamp newerStamp(CreationStamp left, CreationStamp right) {
		if (left != null && right != null) {
			return left.getDate().after(right.getDate()) ? left : right;
		}
		return left == null ? right : left;
	}

	private Map<Integer, CreationStamp> getLatestProduceCreationStamps() {
		Map<Integer, CreationStamp> stamps = new HashMap<>();
		for (Partition partition : m_topic.getPartitions()) {
			try {
				CreationStamp newer = newerStamp(queryLatestProduceCreationStamp(partition.getId(), 1),
				      queryLatestProduceCreationStamp(partition.getId(), 0));
				if (newer != null) {
					stamps.put(partition.getId(), newer);
				}
			} catch (Exception e) {
				log.warn("Query latest message failed: {}", m_topic.getName());
			}
		}
		return stamps;
	}

	@Override
	public void run() {
		try {
			long now = System.currentTimeMillis();
			Map<Integer, CreationStamp> stamps = getLatestProduceCreationStamps();
			Iterator<Entry<Integer, CreationStamp>> iter = stamps.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<Integer, CreationStamp> entry = iter.next();
				CreationStamp stamp = entry.getValue();
				int partitionId = entry.getKey();
				if (m_limitsInMin.get(partitionId) == null
				      || now - stamp.getDate().getTime() < TimeUnit.MINUTES.toMillis(m_limitsInMin.get(partitionId))) {
					iter.remove();
				}
			}
			if (stamps.size() > 0) {
				m_result.addMonitorEvent(generateLongTimeNoProduceEvent(stamps));
			}
		} catch (Exception e) {
			m_exceptions.add(e);
		} finally {
			m_latch.countDown();
		}
	}

	private MonitorEvent generateLongTimeNoProduceEvent(Map<Integer, CreationStamp> stamps) {
		Map<Integer, Pair<Integer, CreationStamp>> map = new HashMap<>();
		for (Entry<Integer, CreationStamp> entry : stamps.entrySet()) {
			map.put(entry.getKey(), new Pair<Integer, CreationStamp>(m_limitsInMin.get(entry.getKey()), entry.getValue()));
		}
		return new LongTimeNoProduceEvent(m_topic.getName(), map);
	}
}
