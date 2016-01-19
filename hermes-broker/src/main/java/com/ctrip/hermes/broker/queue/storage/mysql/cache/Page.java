package com.ctrip.hermes.broker.queue.storage.mysql.cache;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.CollectionUtil;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class Page<T> {

	private long m_pageNo;

	private long m_startOffset;

	private long m_endOffset;

	private AtomicReferenceArray<T> m_datas;

	private long m_loadingIntervalMillis;

	private AtomicBoolean m_loading = new AtomicBoolean(false);

	private AtomicLong m_lastLoadedTime = new AtomicLong(0L);

	private AtomicLong m_latestLoadedOffset = new AtomicLong(-1L);

	private AtomicBoolean m_fresh = new AtomicBoolean(false);

	public Page(long pageNo, int pageSize, long loadingIntervalMillis) {
		m_pageNo = pageNo;
		m_startOffset = m_pageNo * pageSize;
		m_endOffset = (m_pageNo + 1) * pageSize - 1;
		m_datas = new AtomicReferenceArray<>(pageSize);
		m_loadingIntervalMillis = loadingIntervalMillis;
		m_latestLoadedOffset.set(m_startOffset - 1);
	}

	public long getPageNo() {
		return m_pageNo;
	}

	public boolean isFresh() {
		return m_fresh.compareAndSet(false, true);
	}

	public List<T> getDatas(long startOffset, int batchSize) {
		List<T> datas = new LinkedList<>();
		if (startOffset >= m_startOffset && startOffset <= m_endOffset) {
			int count = 0;
			for (int pos = (int) (startOffset - m_startOffset); pos < m_datas.length() && count < batchSize; pos++) {
				T data = m_datas.get(pos);
				if (data != null) {
					datas.add(data);
					count++;
				}
			}
		}
		return datas;
	}

	public void addData(List<Pair<Long, T>> idDataPairs) {
		if (CollectionUtil.isNotEmpty(idDataPairs)) {
			long maxId = -1L;
			for (Pair<Long, T> pair : idDataPairs) {
				int pos = (int) (pair.getKey() - m_startOffset);
				if (pos >= 0 && pos < m_datas.length() && m_datas.get(pos) == null) {
					m_datas.set(pos, pair.getValue());
				}

				maxId = Math.max(maxId, pair.getKey());
			}
			updateLatestLoadedOffset(maxId);
		}
	}

	public void updateLatestLoadedOffset(long offset) {
		if (m_latestLoadedOffset.get() < offset) {
			m_latestLoadedOffset.set(Math.min(m_endOffset, offset));
		}
	}

	public long getLatestLoadedOffset() {
		return m_latestLoadedOffset.get();
	}

	public long getStartOffset() {
		return m_startOffset;
	}

	public long getEndOffset() {
		return m_endOffset;
	}

	public boolean startLoading() {
		return System.currentTimeMillis() - m_lastLoadedTime.get() >= m_loadingIntervalMillis
		      && m_loading.compareAndSet(false, true);
	}

	public void endLoading() {
		m_loading.set(false);
		m_lastLoadedTime.set(System.currentTimeMillis());
	}

	public boolean isComplete() {
		return m_latestLoadedOffset.get() == m_endOffset;
	}

}
