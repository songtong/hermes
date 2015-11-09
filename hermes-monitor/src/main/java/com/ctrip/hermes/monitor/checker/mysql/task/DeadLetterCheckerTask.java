package com.ctrip.hermes.monitor.checker.mysql.task;

import io.netty.util.internal.ConcurrentSet;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.monitor.event.TopicLargeDeadLetterEvent;
import com.ctrip.hermes.metaservice.queue.DeadLetterDao;
import com.ctrip.hermes.metaservice.queue.DeadLetterEntity;
import com.ctrip.hermes.monitor.checker.CheckerResult;

public class DeadLetterCheckerTask implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(DeadLetterCheckerTask.class);

	private Map<Topic, Integer> m_limits;

	private DeadLetterDao m_dao;

	private Date m_from;

	private Date m_to;

	private CheckerResult m_result;

	private CountDownLatch m_latch;

	private ConcurrentSet<Exception> m_exceptions;

	public DeadLetterCheckerTask(Map<Topic, Integer> limits, DeadLetterDao dao, Date from, Date to,
	      CheckerResult result, CountDownLatch latch, ConcurrentSet<Exception> exceptions) {
		m_limits = limits;
		m_dao = dao;
		m_from = from;
		m_to = to;
		m_result = result;
		m_latch = latch;
		m_exceptions = exceptions;
	}

	@Override
	public void run() {
		try {
			for (Entry<Topic, Integer> entry : m_limits.entrySet()) {
				String topic = entry.getKey().getName();
				long tpDeadSum = 0;
				for (Partition partition : entry.getKey().getPartitions()) {
					try {
						tpDeadSum += m_dao.findByTimeRange( //
						      topic, partition.getId(), m_from, m_to, DeadLetterEntity.READSET_COUNT) //
						      .getCountOfTimeRange();
					} catch (DalException e) {
						if (log.isDebugEnabled()) {
							log.debug("Query dead letter count failed, {} {}", topic, partition.getId());
						}
					}
				}
				if (tpDeadSum >= entry.getValue()) {
					m_result.addMonitorEvent(new TopicLargeDeadLetterEvent(topic, tpDeadSum, m_from, m_to));
				}
			}
		} catch (Exception e) {
			m_exceptions.add(e);
		} finally {
			m_latch.countDown();
		}
	}
}