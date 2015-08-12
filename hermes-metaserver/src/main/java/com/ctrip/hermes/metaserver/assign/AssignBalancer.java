package com.ctrip.hermes.metaserver.assign;

import java.util.LinkedList;
import java.util.List;

public class AssignBalancer<T> {
	private int m_total;

	private int m_copies;

	private int m_copiesAllocated = 0;

	private LinkedList<T> m_freeAssigns = new LinkedList<>();

	private int m_avg;

	private int m_biggerThanAvgsAllocated = 0;

	private int m_remainder;

	public AssignBalancer(int total, int copies, List<T> freeAssigns) {
		m_total = total;
		m_copies = copies;

		m_avg = m_total / m_copies;
		m_remainder = m_total % m_copies;

		if (freeAssigns != null) {
			m_freeAssigns.addAll(freeAssigns);
		}
	}

	public List<T> adjust(List<T> originAssign) {
		m_copiesAllocated++;
		LinkedList<T> newAssign = new LinkedList<>(originAssign);

		if (lastCopyOrBeyond()) {
			newAssign.addAll(m_freeAssigns);
		} else {
			int assignsToAdjust = distanceToAvg(originAssign.size());
			if (assignsToAdjust > 0) {
				for (int i = 0; i < assignsToAdjust; i++) {
					if (!m_freeAssigns.isEmpty()) {
						newAssign.add(m_freeAssigns.removeFirst());
					}
				}
			} else if (assignsToAdjust < 0) {
				for (int i = 0; i < Math.abs(assignsToAdjust); i++) {
					if (!newAssign.isEmpty()) {
						m_freeAssigns.add(newAssign.removeFirst());
					}
				}
			}
		}

		if (m_remainder != 0 && newAssign.size() == m_avg + 1) {
			m_biggerThanAvgsAllocated++;
		}

		return newAssign;
	}

	protected int distanceToAvg(int point) {
		if (m_remainder == 0) {
			return m_avg - point;
		} else {
			if (biggerThanAvgAllowed()) {
				return point > m_avg ? m_avg + 1 - point : m_avg - point;
			} else {
				return m_avg - point;
			}
		}
	}

	private boolean biggerThanAvgAllowed() {
		return m_biggerThanAvgsAllocated < m_remainder;
	}

	private boolean lastCopyOrBeyond() {
		return m_copiesAllocated >= m_copies;
	}
}