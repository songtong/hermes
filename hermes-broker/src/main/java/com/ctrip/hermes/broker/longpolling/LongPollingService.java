package com.ctrip.hermes.broker.longpolling;


/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface LongPollingService {

	void schedulePush(PullMessageTask task);

	void stop();
}
