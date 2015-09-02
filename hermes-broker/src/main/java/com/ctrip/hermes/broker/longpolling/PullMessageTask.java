package com.ctrip.hermes.broker.longpolling;

import io.netty.channel.Channel;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;

class PullMessageTask {
	private Tpg m_tpg;

	private long m_correlationId;

	private int m_batchSize;

	private Channel m_channel;

	private long m_expireTime;

	private Lease m_brokerLease;

	private String m_sessionId;

	public PullMessageTask(Tpg tpg, long correlationId, int batchSize, Channel channel, long expireTime,
	      Lease brokerLease, String sessionId) {
		m_tpg = tpg;
		m_correlationId = correlationId;
		m_batchSize = batchSize;
		m_channel = channel;
		m_expireTime = expireTime;
		m_brokerLease = brokerLease;
		m_sessionId = sessionId;
	}

	public long getExpireTime() {
		return m_expireTime;
	}

	public Tpg getTpg() {
		return m_tpg;
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public int getBatchSize() {
		return m_batchSize;
	}

	public Channel getChannel() {
		return m_channel;
	}

	public Lease getBrokerLease() {
		return m_brokerLease;
	}

	public String getSessionId() {
		return m_sessionId;
	}

}