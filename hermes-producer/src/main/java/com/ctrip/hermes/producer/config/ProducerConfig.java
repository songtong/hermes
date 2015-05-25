package com.ctrip.hermes.producer.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ProducerConfig.class)
public class ProducerConfig {

	public int getBrokerSenderTaskExecThreadCount() {
		return 10;
	}

	public long getBrokerSenderCheckInterval() {
		return 50L;
	}

	public int getBrokerSenderBatchSize() {
		return 3000;
	}

	public long getBrokerSenderSendTimeout() {
		return 200;
	}

	public int getBrokerSenderTopicPartitionTaskQueueSize() {
		return 10000;
	}

}
