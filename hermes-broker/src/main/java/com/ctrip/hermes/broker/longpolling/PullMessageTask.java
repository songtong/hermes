package com.ctrip.hermes.broker.longpolling;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

class PullMessageTask {
	private Tpg m_tpg;

	private long m_correlationId;

	private int m_batchSize;

	private EndpointChannel m_channel;

	private long m_expireTime;

	private Lease m_brokerLease;

	public PullMessageTask(Tpg tpg, long correlationId, int batchSize, EndpointChannel channel, long expireTime,
	      Lease brokerLease) {
		m_tpg = tpg;
		m_correlationId = correlationId;
		m_batchSize = batchSize;
		m_channel = channel;
		m_expireTime = expireTime;
		m_brokerLease = brokerLease;
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

	public EndpointChannel getChannel() {
		return m_channel;
	}

	public Lease getBrokerLease() {
		return m_brokerLease;
	}

}