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

	public String getDefaultBrokerSenderNetworkIoCheckIntervalBaseMillis() {
		return "5";
	}

	public String getDefaultBrokerSenderNetworkIoCheckIntervalMaxMillis() {
		return "50";
	}

	public String getDefaultBrokerSenderBatchSize() {
		return "10000";
	}

	public long getDefaultBrokerSenderSendTimeoutMillis() {
		return 10 * 1000;
	}

	public String getDefaultBrokerSenderTaskQueueSize() {
		return "500000";
	}

	public String getDefaultProducerCallbackThreadCount() {
		return "3";
	}

	public long getSendMessageReadResultTimeoutMillis() {
		return 10 * 1000L;
	}

}
