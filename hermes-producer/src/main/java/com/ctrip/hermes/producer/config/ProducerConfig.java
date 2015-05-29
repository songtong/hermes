package com.ctrip.hermes.producer.config;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ProducerConfig.class)
public class ProducerConfig {

	public String getDefaultBrokerSenderNetworkIoThreadCount() {
		return "10";
	}

	public String getDefaultBrokerSenderNetworkIoCheckIntervalMillis() {
		return "50";
	}

	public String getDefaultBrokerSenderBatchSize() {
		return "300";
	}

	public String getDefaultBrokerSenderSendTimeoutMillis() {
		return "200";
	}

	public String getDefaultBrokerSenderTaskQueueSize() {
		return "10000";
	}

	public String getDefaultProducerCallbackThreadCount() {
		return "3";
	}

	public long getSendMessageReadResultTimeoutMillis() {
		return 10 * 1000L;
	}

}
