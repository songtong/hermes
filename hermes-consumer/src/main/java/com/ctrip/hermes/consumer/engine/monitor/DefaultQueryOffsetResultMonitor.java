package com.ctrip.hermes.consumer.engine.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.QueryLatestConsumerOffsetCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = QueryOffsetResultMonitor.class)
public class DefaultQueryOffsetResultMonitor implements QueryOffsetResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultQueryOffsetResultMonitor.class);

	private Map<Long, QueryLatestConsumerOffsetCommand> m_cmds = new ConcurrentHashMap<Long, QueryLatestConsumerOffsetCommand>();

	@Override
	public void monitor(QueryLatestConsumerOffsetCommand cmd) {
		if (cmd != null) {
			m_cmds.put(cmd.getHeader().getCorrelationId(), cmd);
		}
	}

	@Override
	public void resultReceived(QueryOffsetResultCommand result) {
		if (result != null) {
			QueryLatestConsumerOffsetCommand queryOffsetCommand = null;
			queryOffsetCommand = m_cmds.remove(result.getHeader().getCorrelationId());

			if (queryOffsetCommand != null) {
				try {
					queryOffsetCommand.onResultReceived(result);
				} catch (Exception e) {
					log.warn("Exception occurred while calling resultReceived", e);
				}
			}
		}
	}

	@Override
	public void remove(QueryLatestConsumerOffsetCommand cmd) {
		if (cmd != null) {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		}
	}
}
