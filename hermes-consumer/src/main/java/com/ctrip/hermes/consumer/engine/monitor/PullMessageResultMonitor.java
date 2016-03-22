package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.PullMessageResultListener;
import com.ctrip.hermes.core.transport.command.v4.PullMessageResultCommandV4;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PullMessageResultMonitor {

	void monitor(PullMessageResultListener cmd);

	void resultReceived(PullMessageResultCommandV4 ack);

	void remove(PullMessageResultListener cmd);

}
