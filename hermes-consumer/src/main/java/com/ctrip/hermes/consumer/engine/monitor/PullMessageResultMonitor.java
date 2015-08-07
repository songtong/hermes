package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface PullMessageResultMonitor {

	void monitor(PullMessageCommand cmd);

	void resultReceived(PullMessageResultCommand ack);

	void remove(PullMessageCommand cmd);

}
