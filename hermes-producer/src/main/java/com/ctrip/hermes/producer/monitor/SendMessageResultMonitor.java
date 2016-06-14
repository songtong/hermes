package com.ctrip.hermes.producer.monitor;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.v5.SendMessageCommandV5;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface SendMessageResultMonitor {

	Future<Boolean> monitor(SendMessageCommandV5 cmd);

	void cancel(SendMessageCommandV5 cmd);

	void resultReceived(SendMessageResultCommand result);

}
