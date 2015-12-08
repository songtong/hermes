package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.v3.QueryLatestConsumerOffsetCommandV3;
import com.ctrip.hermes.core.transport.command.v3.QueryOffsetResultCommandV3;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface QueryOffsetResultMonitor {
	void monitor(QueryLatestConsumerOffsetCommandV3 cmd);

	void resultReceived(QueryOffsetResultCommandV3 ack);

	void remove(QueryLatestConsumerOffsetCommandV3 cmd);
}
