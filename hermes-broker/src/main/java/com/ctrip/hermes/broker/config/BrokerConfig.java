package com.ctrip.hermes.broker.config;

import java.util.UUID;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerConfig.class)
public class BrokerConfig {
	private String m_sessionId = UUID.randomUUID().toString();

	private long m_leaseRenewTimeMillsBeforeExpire = 2 * 1000L;

	public String getSessionId() {
		return m_sessionId;
	}

	public long getLeaseRenewTimeMillsBeforeExpire() {
		return m_leaseRenewTimeMillsBeforeExpire;
	}
}
