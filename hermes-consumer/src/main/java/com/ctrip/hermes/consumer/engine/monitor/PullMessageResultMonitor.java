package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.PullMessageResultListener;
import com.ctrip.hermes.core.transport.command.v3.PullMessageResultCommandV3;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PullMessageResultMonitor {

	void monitor(PullMessageResultListener cmd);

	void resultReceived(PullMessageResultCommandV3 ack);

	void remove(PullMessageResultListener cmd);

}
