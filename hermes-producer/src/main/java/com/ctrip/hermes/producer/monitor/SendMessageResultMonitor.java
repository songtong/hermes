package com.ctrip.hermes.producer.monitor;

import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface SendMessageResultMonitor {

	void monitor(SendMessageCommand cmd);

	void resultReceived(SendMessageResultCommand result);

}
