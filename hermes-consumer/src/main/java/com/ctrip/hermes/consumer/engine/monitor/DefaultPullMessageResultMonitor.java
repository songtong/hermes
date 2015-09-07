package com.ctrip.hermes.consumer.engine.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = PullMessageResultMonitor.class)
public class DefaultPullMessageResultMonitor implements PullMessageResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultPullMessageResultMonitor.class);

	private Map<Long, PullMessageCommandV2> m_cmds = new ConcurrentHashMap<Long, PullMessageCommandV2>();

	@Override
	public void monitor(PullMessageCommandV2 cmd) {
		if (cmd != null) {
			m_cmds.put(cmd.getHeader().getCorrelationId(), cmd);
		}
	}

	@Override
	public void resultReceived(PullMessageResultCommandV2 result) {
		if (result != null) {
			PullMessageCommandV2 pullMessageCommand = null;
			pullMessageCommand = m_cmds.remove(result.getHeader().getCorrelationId());

			if (pullMessageCommand != null) {
				try {
					pullMessageCommand.onResultReceived(result);
				} catch (Exception e) {
					log.warn("Exception occurred while calling resultReceived", e);
				}
			} else {
				result.release();
			}
		}
	}

	@Override
	public void remove(PullMessageCommandV2 cmd) {
		if (cmd != null) {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		}
	}
}
