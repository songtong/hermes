package com.ctrip.hermes.broker.config;

import java.util.UUID;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.StringUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerConfig.class)
public class BrokerConfig {
	private String m_sessionId = UUID.randomUUID().toString();

	private long m_leaseRenewTimeMillsBeforeExpire = 2 * 1000L;

	private static final int DEFAULT_BROKER_PORT = 4376;

	private static final int DEFAULT_SHUTDOWN_PORT = 4888;

	public String getSessionId() {
		return m_sessionId;
	}

	public String getRegistryName(String name) {
		return "default";
	}

	public String getRegistryBasePath() {
		return "brokers";
	}

	public long getLeaseRenewTimeMillsBeforeExpire() {
		return m_leaseRenewTimeMillsBeforeExpire;
	}

	public int getLongPollingServiceThreadCount() {
		return 10;
	}

	public int getLongPollingCheckIntervalBaseMillis() {
		return 100;
	}

	public int getLongPollingCheckIntervalMaxMillis() {
		return 1000;
	}

	public int getDumperBatchSize() {
		return 10000;
	}

	public int getDumperNoMessageWaitIntervalBaseMillis() {
		return 5;
	}
	
	public int getDumperNoMessageWaitIntervalMaxMillis() {
		return 50;
	}

	public long getAckManagerCheckIntervalMillis() {
		return 10;
	}

	public int getAckManagerOpHandlingBatchSize() {
		return 5000;
	}

	public int getAckManagerOpQueueSize() {
		return 500000;
	}

	public int getLeaseContainerThreadCount() {
		return 10;
	}

	public long getDefaultLeaseRenewDelayMillis() {
		return 500L;
	}

	public long getDefaultLeaseAcquireDelayMillis() {
		return 100L;
	}

	public int getListeningPort() {
		String port = System.getProperty("brokerPort");
		if (!StringUtils.isNumeric(port)) {
			return DEFAULT_BROKER_PORT;
		} else {
			return Integer.valueOf(port);
		}
	}

	public int getClientMaxIdleSeconds() {
		return 3600;
	}

	public int getShutdownRequestPort() {
		String port = System.getProperty("brokerShutdownPort");
		if (!StringUtils.isNumeric(port)) {
			return DEFAULT_SHUTDOWN_PORT;
		} else {
			return Integer.valueOf(port);
		}
	}
}
