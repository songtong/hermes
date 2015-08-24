package com.ctrip.hermes.consumer.integration.assist;

import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;

public class TestConsumerConfig extends ConsumerConfig {
	@Override
	public long getRenewLeaseTimeMillisBeforeExpired() {
		return 100;
	}

	@Override
	public long getDefaultLeaseAcquireDelayMillis() {
		return 100L;
	}

	@Override
	public long getNoMessageWaitIntervalMillis() {
		return 50L;
	}
}
