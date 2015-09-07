package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PullMessageResultMonitor {

	void monitor(PullMessageCommandV2 cmd);

	void resultReceived(PullMessageResultCommandV2 ack);

	void remove(PullMessageCommandV2 cmd);

}
