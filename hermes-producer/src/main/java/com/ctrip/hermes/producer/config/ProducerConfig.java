package com.ctrip.hermes.producer.config;

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
@Named(type = ProducerConfig.class)
public class ProducerConfig implements Initializable {

	public static final int DEFAULT_BROKER_SENDER_NETWORK_IO_THREAD_COUNT = 10;

	public static final long DEFAULT_BROKER_SENDER_ACCEPT_TIMEOUT = 4 * 1000L;

	public static final long DEFAULT_BROKER_SENDER_RESULT_TIMEOUT = 4 * 1000L;

	public static final int DEFAULT_BROKER_SENDER_TASK_QUEUE_SIZE = 200000;

	public static final int DEFAULT_BROKER_SENDER_BATCH_SIZE = 500;

	public static final int DEFAULT_BROKER_SENDER_NETWORK_IO_CHECK_INTERVAL_BASE_MILLIS = 10;

	public static final int DEFAULT_BROKER_SENDER_NETWORK_IO_CHECK_INTERVAL_MAX_MILLIS = 10;

	public static final int DEFAULT_PRODUCER_CALLBACK_THREAD_COUNT = 10;

	private static final int DEFAULT_BROKER_SENDER_CONCURRENT_LEVEL = 2;

	@Inject
	private ClientEnvironment m_clientEnv;

	private int m_brokerSenderNetworkIoThreadCount = DEFAULT_BROKER_SENDER_NETWORK_IO_THREAD_COUNT;

	private long m_brokerSenderAcceptTimeout = DEFAULT_BROKER_SENDER_ACCEPT_TIMEOUT;

	private long m_brokerSenderResultTimeout = DEFAULT_BROKER_SENDER_RESULT_TIMEOUT;

	private int m_ProducerCallbackThreadCount = DEFAULT_PRODUCER_CALLBACK_THREAD_COUNT;

	private int m_brokerSenderTaskQueueSize = DEFAULT_BROKER_SENDER_TASK_QUEUE_SIZE;

	private int m_brokerSenderBatchSize = DEFAULT_BROKER_SENDER_BATCH_SIZE;

	private int m_brokerSenderNetworkIoCheckIntervalBaseMillis = DEFAULT_BROKER_SENDER_NETWORK_IO_CHECK_INTERVAL_BASE_MILLIS;

	private int m_brokerSenderNetworkIoCheckIntervalMaxMillis = DEFAULT_BROKER_SENDER_NETWORK_IO_CHECK_INTERVAL_MAX_MILLIS;

	private boolean m_logEnrichInfoEnabled = false;

	private boolean m_catEnabled = true;

	private int m_brokerSenderConcurrentLevel = DEFAULT_BROKER_SENDER_CONCURRENT_LEVEL;

	@Override
	public void initialize() throws InitializationException {
		String brokerSenderNetworkIoThreadCountStr = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.networkio.threadcount");
		if (StringUtils.isNumeric(brokerSenderNetworkIoThreadCountStr)) {
			m_brokerSenderNetworkIoThreadCount = Integer.valueOf(brokerSenderNetworkIoThreadCountStr);
		}

		String brokerSenderTaskQueueSizeStr = m_clientEnv.getGlobalConfig().getProperty("producer.sender.taskqueue.size");
		if (StringUtils.isNumeric(brokerSenderTaskQueueSizeStr)) {
			m_brokerSenderTaskQueueSize = Integer.valueOf(brokerSenderTaskQueueSizeStr);
		}

		String brokerSenderNetworkIoCheckIntervalBaseMillisStr = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.networkio.interval.base");
		if (StringUtils.isNumeric(brokerSenderNetworkIoCheckIntervalBaseMillisStr)) {
			m_brokerSenderNetworkIoCheckIntervalBaseMillis = Integer
			      .valueOf(brokerSenderNetworkIoCheckIntervalBaseMillisStr);
		}

		String brokerSenderNetworkIoCheckIntervalMaxMillisStr = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.networkio.interval.max");
		if (StringUtils.isNumeric(brokerSenderNetworkIoCheckIntervalMaxMillisStr)) {
			m_brokerSenderNetworkIoCheckIntervalMaxMillis = Integer
			      .valueOf(brokerSenderNetworkIoCheckIntervalMaxMillisStr);
		}

		String producerCallbackThreadCountStr = m_clientEnv.getGlobalConfig()
		      .getProperty("producer.callback.threadcount");
		if (StringUtils.isNumeric(producerCallbackThreadCountStr)) {
			m_ProducerCallbackThreadCount = Integer.valueOf(producerCallbackThreadCountStr);
		}

		String brokerSenderBatchSizeStr = m_clientEnv.getGlobalConfig().getProperty("producer.sender.batchsize");
		if (StringUtils.isNumeric(brokerSenderBatchSizeStr)) {
			m_brokerSenderBatchSize = Integer.valueOf(brokerSenderBatchSizeStr);
		}

		// FIXME rename this ugly name
		String logEnrichInfoEnabledStr = m_clientEnv.getGlobalConfig().getProperty("logEnrichInfo", "false");
		if ("true".equalsIgnoreCase(logEnrichInfoEnabledStr)) {
			m_logEnrichInfoEnabled = true;
		}

		String catEnabled = m_clientEnv.getGlobalConfig().getProperty("producer.cat.enable");
		if (!StringUtils.isBlank(catEnabled)) {
			m_catEnabled = !"false".equalsIgnoreCase(catEnabled);
		}

		String senderConcurrentLevelStr = m_clientEnv.getGlobalConfig().getProperty("producer.concurrent.level");
		if (StringUtils.isNumeric(senderConcurrentLevelStr)) {
			m_brokerSenderConcurrentLevel = Integer.valueOf(senderConcurrentLevelStr);
		}
		String brokerSenderAcceptTimeout = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.sender.accept.timeout.millis");
		if (StringUtils.isNumeric(brokerSenderAcceptTimeout)) {
			m_brokerSenderAcceptTimeout = Integer.valueOf(brokerSenderAcceptTimeout);
		}
		String brokerSenderResultTimeout = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.sender.result.timeout.millis");
		if (StringUtils.isNumeric(brokerSenderResultTimeout)) {
			m_brokerSenderResultTimeout = Integer.valueOf(brokerSenderResultTimeout);
		}

		// FIXME log config loading details.
	}

	public boolean isCatEnabled() {
		return m_catEnabled;
	}

	public int getBrokerSenderNetworkIoThreadCount() {
		return m_brokerSenderNetworkIoThreadCount;
	}

	public int getBrokerSenderNetworkIoCheckIntervalBaseMillis() {
		return m_brokerSenderNetworkIoCheckIntervalBaseMillis;
	}

	public int getBrokerSenderNetworkIoCheckIntervalMaxMillis() {
		return m_brokerSenderNetworkIoCheckIntervalMaxMillis;
	}

	public int getBrokerSenderBatchSize() {
		return m_brokerSenderBatchSize;
	}

	public long getBrokerSenderAcceptTimeoutMillis() {
		return m_brokerSenderAcceptTimeout;
	}

	public int getBrokerSenderTaskQueueSize() {
		return m_brokerSenderTaskQueueSize;
	}

	public int getProducerCallbackThreadCount() {
		return m_ProducerCallbackThreadCount;
	}

	public long getBrokerSenderResultTimeoutMillis() {
		return m_brokerSenderResultTimeout;
	}

	public boolean isLogEnrichInfoEnabled() {
		return m_logEnrichInfoEnabled;
	}

	public int getBrokerSenderConcurrentLevel() {
		return m_brokerSenderConcurrentLevel;
	}

	public int getProduceTimeoutSeconds(String topic) {
		try {
			String produceTimeout = m_clientEnv.getProducerConfig(topic).getProperty("produce.timeout.seconds");
			if (StringUtils.isNumeric(produceTimeout)) {
				return Integer.valueOf(produceTimeout);
			}
		} catch (Exception e) {
			// ignore
		}
		return -1;
	}

}
