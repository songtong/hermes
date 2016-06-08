package com.ctrip.hermes.consumer.engine.config;

import java.io.IOException;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.StringUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerConfig.class)
public class ConsumerConfig implements Initializable {

	public static final int DEFAULT_LOCALCACHE_SIZE = 200;

	public static final int DEFAULT_MAX_ACK_HOLDER_SIZE = 50000;

	private static final int DEFAULT_NOTIFIER_THREAD_COUNT = 1;

	private static final int DEFAULT_NOTIFIER_WORK_QUEUE_SIZE = 1;

	private static final int DEFAULT_ACK_CHECKER_INTERVAL_MILLIS = 1000;

	private static final int DEFAULT_ACK_CHECKER_IO_THREAD_COUNT = 3;

	private static final int DEFAULT_ACK_CHECKER_IO_TIMEOUT_MILLIS = 4000;

	private static final int DEFAULT_ACK_CHECKER_ACCEPT_TIMEOUT_MILLIS = 4000;

	private static final int DEFAULT_NO_MESSAGE_WAIT_BASE_MILLIS = 10;

	private static final int DEFAULT_NO_MESSAGE_WAIT_MAX_MILLIS = 10;

	private static final int DEFAULT_ACK_COMMAND_MAX_SIZE = 5000;

	@Inject
	private ClientEnvironment m_env;

	@Inject
	private MetaService m_metaService;

	private int m_ackCheckerIntervalMillis = DEFAULT_ACK_CHECKER_INTERVAL_MILLIS;

	private int m_ackCheckerIoThreadCount = DEFAULT_ACK_CHECKER_IO_THREAD_COUNT;

	private int m_ackCheckerIoTimeoutMillis = DEFAULT_ACK_CHECKER_IO_TIMEOUT_MILLIS;

	private int m_ackCheckerAcceptTimeoutMillis = DEFAULT_ACK_CHECKER_ACCEPT_TIMEOUT_MILLIS;

	private int m_noMessageWaitBaseMillis = DEFAULT_NO_MESSAGE_WAIT_BASE_MILLIS;

	private int m_noMessageWaitMaxMillis = DEFAULT_NO_MESSAGE_WAIT_MAX_MILLIS;

	private int m_ackCommandMaxSize = DEFAULT_ACK_COMMAND_MAX_SIZE;

	public int getLocalCacheSize(String topic) throws IOException {
		String localCacheSizeStr = m_env.getConsumerConfig(topic).getProperty("consumer.localcache.size");

		if (StringUtils.isNumeric(localCacheSizeStr)) {
			return Integer.valueOf(localCacheSizeStr);
		}

		return DEFAULT_LOCALCACHE_SIZE;
	}

	public long getRenewLeaseTimeMillisBeforeExpired() {
		return 5 * 1000L;
	}

	public long getStopConsumerTimeMillsBeforLeaseExpired() {
		return getRenewLeaseTimeMillisBeforeExpired() - 3 * 1000L;
	}

	public long getDefaultLeaseAcquireDelayMillis() {
		return 500L;
	}

	public long getDefaultLeaseRenewDelayMillis() {
		return 500L;
	}

	public int getNoMessageWaitBaseMillis() {
		return m_noMessageWaitBaseMillis;
	}

	public int getNoMessageWaitMaxMillis() {
		return m_noMessageWaitMaxMillis;
	}

	public int getNoEndpointWaitBaseMillis() {
		return 500;
	}

	public int getNoEndpointWaitMaxMillis() {
		return 4000;
	}

	public int getNotifierThreadCount(String topic) throws IOException {
		String threadCountStr = m_env.getConsumerConfig(topic).getProperty("consumer.notifier.threadcount");
		if (StringUtils.isNumeric(threadCountStr)) {
			return Integer.valueOf(threadCountStr);
		}

		return DEFAULT_NOTIFIER_THREAD_COUNT;
	}

	public long getQueryOffsetTimeoutMillis() {
		return 3000;
	}

	public long getPullMessageTimeoutMills() {
		return 30000;
	}

	public long getPartitionWatchdogIntervalSeconds() {
		return 30;
	}

	public int getMaxAckHolderSize(String topicName) throws IOException {
		String maxAckHolderSizeStr = m_env.getConsumerConfig(topicName).getProperty("consumer.max.ack.holder.size");

		if (StringUtils.isNumeric(maxAckHolderSizeStr)) {
			return Integer.valueOf(maxAckHolderSizeStr);
		}

		return DEFAULT_MAX_ACK_HOLDER_SIZE;
	}

	@Override
	public void initialize() throws InitializationException {
		String ackCheckerIntervalMillis = m_env.getGlobalConfig().getProperty("consumer.ack.checker.interval.millis");
		if (StringUtils.isNumeric(ackCheckerIntervalMillis)) {
			m_ackCheckerIntervalMillis = Integer.valueOf(ackCheckerIntervalMillis);
		}

		String ackCheckerIoThreadCount = m_env.getGlobalConfig().getProperty("consumer.ack.checker.io.thread.count");
		if (StringUtils.isNumeric(ackCheckerIoThreadCount)) {
			m_ackCheckerIoThreadCount = Integer.valueOf(ackCheckerIoThreadCount);
		}

		String ackCheckerIoTimeoutMillis = m_env.getGlobalConfig().getProperty("consumer.ack.checker.io.timeout.millis");
		if (StringUtils.isNumeric(ackCheckerIoTimeoutMillis)) {
			m_ackCheckerIoTimeoutMillis = Integer.valueOf(ackCheckerIoTimeoutMillis);
		}

		String noMessageWaitBaseMillis = m_env.getGlobalConfig().getProperty("consumer.no.message.wait.base.millis");
		if (StringUtils.isNumeric(noMessageWaitBaseMillis)) {
			m_noMessageWaitBaseMillis = Integer.valueOf(noMessageWaitBaseMillis);
		}

		String noMessageWaitMaxMillis = m_env.getGlobalConfig().getProperty("consumer.no.message.wait.max.millis");
		if (StringUtils.isNumeric(noMessageWaitMaxMillis)) {
			m_noMessageWaitMaxMillis = Integer.valueOf(noMessageWaitMaxMillis);
		}

		String maxAckCmdSize = m_env.getGlobalConfig().getProperty("consumer.ack.max.cmd.size");
		if (StringUtils.isNumeric(maxAckCmdSize)) {
			m_ackCommandMaxSize = Integer.valueOf(maxAckCmdSize);
		}
	}

	public int getAckCheckerIntervalMillis() {
		return m_ackCheckerIntervalMillis;
	}

	public int getAckCheckerIoThreadCount() {
		return m_ackCheckerIoThreadCount;
	}

	public int getAckCheckerIoTimeoutMillis() {
		return m_ackCheckerIoTimeoutMillis;
	}

	public int getAckCheckerAcceptTimeoutMillis() {
		return m_ackCheckerAcceptTimeoutMillis;
	}

	public int getNotifierWorkQueueSize(String topic) throws IOException {
		String workQueueSizeStr = m_env.getConsumerConfig(topic).getProperty("consumer.notifier.work.queue.size");
		if (StringUtils.isNumeric(workQueueSizeStr)) {
			return Integer.valueOf(workQueueSizeStr);
		}
		return DEFAULT_NOTIFIER_WORK_QUEUE_SIZE;
	}

	public int getAckCommandMaxSize() {
		return m_ackCommandMaxSize;
	}

}
