package com.ctrip.hermes.broker.config;

import java.util.UUID;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.StringUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerConfig.class)
public class BrokerConfig implements Initializable {
	@Inject
	private ClientEnvironment m_env;

	private String m_sessionId = UUID.randomUUID().toString();

	private long m_leaseRenewTimeMillsBeforeExpire = 2 * 1000L;

	private static final int DEFAULT_DUMPER_BATCH_SIZE = 5000;

	private static final int DEFAULT_MYSQL_BATCH_INSERT_SIZE = 2000;

	private int m_dumperBatchSzie = DEFAULT_DUMPER_BATCH_SIZE;

	private int m_mySQLBatchInsertSzie = DEFAULT_MYSQL_BATCH_INSERT_SIZE;

	private static final int DEFAULT_BROKER_PORT = 4376;

	private static final int DEFAULT_SHUTDOWN_PORT = 4888;

	@Override
	public void initialize() throws InitializationException {
		String dumperBatchSizeStr = m_env.getGlobalConfig().getProperty("broker.dumper.batch.size");
		if (StringUtils.isNumeric(dumperBatchSizeStr)) {
			m_dumperBatchSzie = Integer.valueOf(dumperBatchSizeStr);
		}
		String mysqlBatchInsertSizeStr = m_env.getGlobalConfig().getProperty("broker.mysql.batch.size");
		if (StringUtils.isNumeric(mysqlBatchInsertSizeStr)) {
			m_mySQLBatchInsertSzie = Integer.valueOf(mysqlBatchInsertSizeStr);
		}
	}

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
		return 50;
	}

	public int getLongPollingCheckIntervalBaseMillis() {
		return 100;
	}

	public int getLongPollingCheckIntervalMaxMillis() {
		return 500;
	}

	public int getDumperBatchSize() {
		return m_dumperBatchSzie;
	}

	public int getMySQLBatchInsertSize() {
		return m_mySQLBatchInsertSzie;
	}

	public int getDumperNoMessageWaitIntervalBaseMillis() {
		return 5;
	}

	public int getDumperNoMessageWaitIntervalMaxMillis() {
		return 50;
	}

	public long getAckOpCheckIntervalMillis() {
		return 200;
	}

	public int getAckOpHandlingBatchSize() {
		return 5000;
	}

	public int getAckOpExecutorThreadCount() {
		return 10;
	}

	public int getAckOpQueueSize() {
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
