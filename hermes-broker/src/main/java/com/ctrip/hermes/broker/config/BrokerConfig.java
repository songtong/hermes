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

	private int m_longPollingServiceThreadCount = 50;

	private static final int DEFAULT_MESSAGE_QUEUE_FLUSH_BATCH_SIZE = 5000;

	private static final int DEFAULT_MYSQL_BATCH_INSERT_SIZE = 200;

	private int m_messageQueueFlushBatchSzie = DEFAULT_MESSAGE_QUEUE_FLUSH_BATCH_SIZE;

	private int m_mySQLBatchInsertSzie = DEFAULT_MYSQL_BATCH_INSERT_SIZE;

	private static final int DEFAULT_BROKER_PORT = 4376;

	private static final int DEFAULT_SHUTDOWN_PORT = 4888;

	private MySQLCacheConfig m_cacheConfig = new MySQLCacheConfig();

	@Override
	public void initialize() throws InitializationException {
		String flushBatchSizeStr = m_env.getGlobalConfig().getProperty("broker.flush.batch.size");
		if (StringUtils.isNumeric(flushBatchSizeStr)) {
			m_messageQueueFlushBatchSzie = Integer.valueOf(flushBatchSizeStr);
		}
		String mysqlBatchInsertSizeStr = m_env.getGlobalConfig().getProperty("broker.mysql.batch.size");
		if (StringUtils.isNumeric(mysqlBatchInsertSizeStr)) {
			m_mySQLBatchInsertSzie = Integer.valueOf(mysqlBatchInsertSizeStr);
		}
		String longPollingServiceThreadCount = m_env.getGlobalConfig().getProperty(
		      "broker.long.polling.service.thread.count");
		if (StringUtils.isNumeric(longPollingServiceThreadCount)) {
			m_longPollingServiceThreadCount = Integer.valueOf(longPollingServiceThreadCount);
		}

		m_cacheConfig.init(m_env.getGlobalConfig());
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
		return m_longPollingServiceThreadCount;
	}

	public int getLongPollingCheckIntervalBaseMillis() {
		return 100;
	}

	public int getLongPollingCheckIntervalMaxMillis() {
		return 500;
	}

	public int getMessageQueueFlushBatchSize() {
		return m_messageQueueFlushBatchSzie;
	}

	public int getMySQLBatchInsertSize() {
		return m_mySQLBatchInsertSzie;
	}

	public int getFlushCheckerNoMessageWaitIntervalBaseMillis() {
		return 5;
	}

	public int getFlushCheckerNoMessageWaitIntervalMaxMillis() {
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

	public int getMessageOffsetQueryPrecisionMillis() {
		return 30000;
	}

	public int getFetchMessageWithOffsetBatchSize() {
		return 500;
	}

	public int getAckMessagesTaskQueueSize() {
		return 500000;
	}

	public int getAckMessagesTaskExecutorThreadCount() {
		return 10;
	}

	public long getAckMessagesTaskExecutorCheckIntervalMillis() {
		return 100;
	}

	public MySQLCacheConfig getMySQLCacheConfig() {
		return m_cacheConfig;
	}
}
