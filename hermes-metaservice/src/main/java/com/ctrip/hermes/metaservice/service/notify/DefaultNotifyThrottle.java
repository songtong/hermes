package com.ctrip.hermes.metaservice.service.notify;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.tuple.Pair;

public class DefaultNotifyThrottle implements NotifyThrottle {
	private static float DFT_DEVIATION = 0.1f;

	private long m_limit;

	private long m_intervalMillis;

	private long m_checkIntervalMillis;

	private volatile long m_lastCleanTime;

	private long m_segmentCount;

	private AtomicLong m_counter = new AtomicLong();

	private AtomicReference<ConcurrentLinkedDeque<Pair<AtomicLong, AtomicLong>>> m_segments;

	private AtomicInteger m_actuallySegmentCount = new AtomicInteger();

	protected DefaultNotifyThrottle(long limit, long intervalMillis, int segmentCount) {
		m_limit = limit;
		m_intervalMillis = intervalMillis;
		m_segmentCount = segmentCount;
		m_checkIntervalMillis = m_intervalMillis / m_segmentCount;

		m_segments = new AtomicReference<>(new ConcurrentLinkedDeque<Pair<AtomicLong, AtomicLong>>());
		m_segments.get().addLast(
		      new Pair<AtomicLong, AtomicLong>(new AtomicLong(System.currentTimeMillis()), new AtomicLong(0)));
		m_actuallySegmentCount.incrementAndGet();
	}

	@Override
	public boolean hit() {
		if (m_counter.getAndIncrement() < m_limit) {
			Pair<AtomicLong, AtomicLong> last = m_segments.get().getLast();
			last.getKey().set(System.currentTimeMillis());
			last.getValue().incrementAndGet();
			return true;
		} else {
			m_counter.decrementAndGet();
			return false;
		}
	}

	public long getCheckIntervalMillis() {
		return m_checkIntervalMillis;
	}

	public synchronized void update(long checkIntervalMillis) {
		long now = System.currentTimeMillis();
		if (isCheckPoint(checkIntervalMillis, now)) {
			m_segments.get().addLast(
			      new Pair<AtomicLong, AtomicLong>(new AtomicLong(System.currentTimeMillis()), new AtomicLong()));
			if (m_actuallySegmentCount.incrementAndGet() > m_segmentCount) {
				m_counter.addAndGet(-m_segments.get().removeFirst().getValue().get());
			}
			m_lastCleanTime = now;
		}
	}

	private boolean isCheckPoint(long checkIntervalMillis, long now) {
		return (now - m_lastCleanTime > m_checkIntervalMillis)
		      || (m_lastCleanTime + m_checkIntervalMillis - now < m_checkIntervalMillis * DFT_DEVIATION)
		      || m_segments.get().getFirst().getKey().get() <= now - m_intervalMillis;
	}
}
