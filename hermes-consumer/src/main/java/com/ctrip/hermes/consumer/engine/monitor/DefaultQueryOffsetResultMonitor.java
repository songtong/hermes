package com.ctrip.hermes.consumer.engine.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.v3.QueryLatestConsumerOffsetCommandV3;
import com.ctrip.hermes.core.transport.command.v3.QueryOffsetResultCommandV3;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = QueryOffsetResultMonitor.class)
public class DefaultQueryOffsetResultMonitor implements QueryOffsetResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultQueryOffsetResultMonitor.class);

	private Map<Long, QueryLatestConsumerOffsetCommandV3> m_cmds = new ConcurrentHashMap<Long, QueryLatestConsumerOffsetCommandV3>();

	@Override
	public void monitor(QueryLatestConsumerOffsetCommandV3 cmd) {
		if (cmd != null) {
			m_cmds.put(cmd.getHeader().getCorrelationId(), cmd);
		}
	}

	@Override
	public void resultReceived(QueryOffsetResultCommandV3 result) {
		if (result != null) {
			QueryLatestConsumerOffsetCommandV3 queryOffsetCommand = null;
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
	public void remove(QueryLatestConsumerOffsetCommandV3 cmd) {
		if (cmd != null) {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		}
	}
}