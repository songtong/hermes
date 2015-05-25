package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PullMessageResultMonitor {

	void monitor(PullMessageCommand cmd);

	void resultReceived(PullMessageResultCommand ack);

}
