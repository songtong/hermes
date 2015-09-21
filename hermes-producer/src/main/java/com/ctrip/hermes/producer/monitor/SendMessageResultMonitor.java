package com.ctrip.hermes.producer.monitor;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface SendMessageResultMonitor {

	Future<Boolean> monitor(SendMessageCommand cmd);

	void cancel(SendMessageCommand cmd);

	void resultReceived(SendMessageResultCommand result);

}
