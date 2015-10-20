package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.QueryLatestConsumerOffsetCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface QueryOffsetResultMonitor {

	void monitor(QueryLatestConsumerOffsetCommand cmd);

	void resultReceived(QueryOffsetResultCommand ack);

	void remove(QueryLatestConsumerOffsetCommand cmd);

}
