package com.ctrip.hermes.consumer.api;


public class OffsetAndMetadata {

	private Long m_priorityOffset;

	private Long m_nonPriorityOffset;

	private Long m_resendOffset;

	public void setPriorityOffset(long priorityOffset) {
		m_priorityOffset = priorityOffset;
	}

	public void setNonPriorityOffset(long nonPriorityOffset) {
		m_nonPriorityOffset = nonPriorityOffset;
	}

	public void setResendOffset(long resendOffset) {
		m_resendOffset = resendOffset;
	}

	public long getPriorityOffset() {
		return m_priorityOffset;
	}

	public long getNonPriorityOffset() {
		return m_nonPriorityOffset;
	}

	public long getResendOffset() {
		return m_resendOffset;
	}

}
