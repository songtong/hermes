package com.ctrip.hermes.core.message;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BaseConsumerMessageAware<T> {
	public BaseConsumerMessage<T> getBaseConsumerMessage();
}
