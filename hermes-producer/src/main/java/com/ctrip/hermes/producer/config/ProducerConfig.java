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

	public static final long DEFAULT_BROKER_SENDER_SEND_TIMEOUT = 10 * 1000L;

	public static final long DEFAULT_BROKER_SENDER_READ_TIMEOUT = 10 * 1000L;

	public static final int DEFAULT_BROKER_SENDER_TASK_QUEUE_SIZE = 500000;

	public static final int DEFAULT_BROKER_SENDER_BATCH_SIZE = 500;

	public static final int DEFAULT_BROKER_SENDER_NETWORK_IO_CHECK_INTERVAL_BASE_MILLIS = 5;

	public static final int DEFAULT_BROKER_SENDER_NETWORK_IO_CHECK_INTERVAL_MAX_MILLIS = 50;

	public static final int DEFAULT_PRODUCER_CALLBACK_THREAD_COUNT = 50;

	@Inject
	private ClientEnvironment m_clientEnv;

	private int m_brokerSenderNetworkIoThreadCount = DEFAULT_BROKER_SENDER_NETWORK_IO_THREAD_COUNT;

	private long m_brokerSenderSendTimeout = DEFAULT_BROKER_SENDER_SEND_TIMEOUT;

	private long m_brokerSenderReadTimeout = DEFAULT_BROKER_SENDER_READ_TIMEOUT;

	private int m_ProducerCallbackThreadCount = DEFAULT_PRODUCER_CALLBACK_THREAD_COUNT;

	private int m_brokerSenderTaskQueueSize = DEFAULT_BROKER_SENDER_TASK_QUEUE_SIZE;

	private int m_brokerSenderBatchSize = DEFAULT_BROKER_SENDER_BATCH_SIZE;

	private int m_brokerSenderNetworkIoCheckIntervalBaseMillis = DEFAULT_BROKER_SENDER_NETWORK_IO_CHECK_INTERVAL_BASE_MILLIS;

	private int m_brokerSenderNetworkIoCheckIntervalMaxMillis = DEFAULT_BROKER_SENDER_NETWORK_IO_CHECK_INTERVAL_MAX_MILLIS;

	private boolean m_logEnrichInfoEnabled = false;

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

		// FIXME log config loading details.
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

	public long getBrokerSenderSendTimeoutMillis() {
		return m_brokerSenderSendTimeout;
	}

	public int getBrokerSenderTaskQueueSize() {
		return m_brokerSenderTaskQueueSize;
	}

	public int getProducerCallbackThreadCount() {
		return m_ProducerCallbackThreadCount;
	}

	public long getSendMessageReadResultTimeoutMillis() {
		return m_brokerSenderReadTimeout;
	}

	public boolean isLogEnrichInfoEnabled() {
		return m_logEnrichInfoEnabled;
	}

}
