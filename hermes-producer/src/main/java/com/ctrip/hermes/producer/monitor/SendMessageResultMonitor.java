package com.ctrip.hermes.producer.monitor;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.v3.SendMessageCommandV3;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface SendMessageResultMonitor {

	Future<Boolean> monitor(SendMessageCommandV3 cmd);

	void cancel(SendMessageCommandV3 cmd);

	void resultReceived(SendMessageResultCommand result);

}
