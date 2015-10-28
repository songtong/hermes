package com.ctrip.hermes.consumer.api;

import com.ctrip.hermes.core.bo.Offset;

public class MessageStreamOffset {
	private long m_priorityOffset;

	private long m_nonPriorityOffset;

	public MessageStreamOffset() {
		this(-1, -1);
	}

	public MessageStreamOffset(Offset offset) {
		this(offset == null ? -1 : offset.getPriorityOffset(), offset == null ? -1 : offset.getNonPriorityOffset());
	}

	public MessageStreamOffset(long priorityOffset, long nonPriorityOffset) {
		m_priorityOffset = priorityOffset;
		m_nonPriorityOffset = nonPriorityOffset;
	}

	public long getPriorityOffset() {
		return m_priorityOffset;
	}

	public void setPriorityOffset(long priorityOffset) {
		m_priorityOffset = priorityOffset;
	}

	public long getNonPriorityOffset() {
		return m_nonPriorityOffset;
	}

	public void setNonPriorityOffset(long nonPriorityOffset) {
		m_nonPriorityOffset = nonPriorityOffset;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (m_nonPriorityOffset ^ (m_nonPriorityOffset >>> 32));
		result = prime * result + (int) (m_priorityOffset ^ (m_priorityOffset >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageStreamOffset other = (MessageStreamOffset) obj;
		if (m_nonPriorityOffset != other.m_nonPriorityOffset)
			return false;
		if (m_priorityOffset != other.m_priorityOffset)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MessageStreamOffset [m_priorityOffset=" + m_priorityOffset + ", m_nonPriorityOffset="
		      + m_nonPriorityOffset + "]";
	}
}
