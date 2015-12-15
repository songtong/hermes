package com.ctrip.hermes.monitor.checker.mysql.task;

import io.netty.util.internal.ConcurrentSet;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.monitor.event.LongTimeNoProduceEvent;
import com.ctrip.hermes.metaservice.queue.CreationStamp;
import com.ctrip.hermes.metaservice.queue.MessagePriority;
import com.ctrip.hermes.metaservice.queue.MessagePriorityDao;
import com.ctrip.hermes.metaservice.queue.MessagePriorityEntity;
import com.ctrip.hermes.monitor.checker.CheckerResult;

public class LongTimeNoProduceCheckerTask implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(LongTimeNoProduceCheckerTask.class);

	private MessagePriorityDao m_dao;

	private CountDownLatch m_latch;

	private Topic m_topic;

	private int m_limitInMin;

	private CheckerResult m_result;

	private ConcurrentSet<Exception> m_exceptions;

	public LongTimeNoProduceCheckerTask(Topic topic, int limitInMin, MessagePriorityDao dao, CheckerResult result,
	      CountDownLatch latch, ConcurrentSet<Exception> exceptions) {
		m_latch = latch;
		m_dao = dao;
		m_topic = topic;
		m_limitInMin = limitInMin;
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

	private Pair<Integer, CreationStamp> getLatestProduceTime() {
		int latestPartitionId = -1;
		CreationStamp latestCreationStamp = null;
		try {
			for (Partition partition : m_topic.getPartitions()) {
				CreationStamp newer = newerStamp(queryLatestProduceCreationStamp(partition.getId(), 1),
				      newerStamp(queryLatestProduceCreationStamp(partition.getId(), 0), latestCreationStamp));
				if (newer != null && !newer.equals(latestCreationStamp)) {
					latestCreationStamp = newer;
					latestPartitionId = partition.getId();
				}
			}
		} catch (Exception e) {
			log.warn("Query latest message failed: {}", m_topic.getName());
		}
		return latestPartitionId == -1 ? null : new Pair<Integer, CreationStamp>(latestPartitionId, latestCreationStamp);
	}

	@Override
	public void run() {
		try {
			long now = System.currentTimeMillis();
			Pair<Integer, CreationStamp> latest = getLatestProduceTime();
			if (latest != null && now - latest.getValue().getDate().getTime() >= TimeUnit.MINUTES.toMillis(m_limitInMin)) {
				m_result.addMonitorEvent(new LongTimeNoProduceEvent(m_topic.getName(), latest.getKey(), latest.getValue()));
			}
		} catch (Exception e) {
			m_exceptions.add(e);
		} finally {
			m_latch.countDown();
		}
	}
}
