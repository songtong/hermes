package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.v2.PullMessageResultListener;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PullMessageResultMonitor {

	void monitor(PullMessageResultListener cmd);

	void resultReceived(PullMessageResultCommandV2 ack);

	void remove(PullMessageResultListener cmd);

}
