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

	public int getLongPollingServiceThreadCount() {
		return 3;
	}

	public long getLongPollingCheckInterval() {
		return 100L;
	}

	public int getDumperFetchSize() {
		return 20;
	}

	public long getDumperNoMessageWaitInterval() {
		return 50L;
	}

	public long getAckManagerCheckInterval() {
		return 100;
	}

	public int getAckManagerOperationHandlingBatchSize() {
		return 50;
	}

	public int getAckManagerOperationQueueSize() {
		return 10000;
	}

	public int getLeaseContainerThreadCount() {
		return 10;
	}

	public long getDefaultLeaseRenewDelay() {
		return 500L;
	}

	public long getDefaultLeaseAcquireDelay() {
		return 100L;
	}

	public int getListenPort() {
		return 4376;
	}
}
