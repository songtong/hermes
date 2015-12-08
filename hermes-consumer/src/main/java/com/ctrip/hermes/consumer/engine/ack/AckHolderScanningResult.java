package com.ctrip.hermes.consumer.engine.ack;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckHolderScanningResult<T> {
	public List<T> m_acked = new LinkedList<>();

	public List<T> m_nacked = new LinkedList<>();

	public void addAcked(T item) {
		m_acked.add(item);
	}

	public void addNacked(T item) {
		m_nacked.add(item);
	}

	public List<T> getAcked() {
		return m_acked;
	}

	public List<T> getNacked() {
		return m_nacked;
	}

}
