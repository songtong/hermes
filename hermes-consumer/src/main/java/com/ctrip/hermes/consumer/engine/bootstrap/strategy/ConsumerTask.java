package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ConsumerTask {

	public void start();

	public void close();
}
