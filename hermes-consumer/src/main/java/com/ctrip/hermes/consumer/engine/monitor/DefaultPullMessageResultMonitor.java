package com.ctrip.hermes.consumer.engine.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = PullMessageResultMonitor.class)
public class DefaultPullMessageResultMonitor implements PullMessageResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultPullMessageResultMonitor.class);

	@Inject
	private SystemClockService m_systemClockService;

	private Map<Long, PullMessageCommand> m_cmds = new ConcurrentHashMap<Long, PullMessageCommand>();

	@Override
	public void monitor(PullMessageCommand cmd) {
		if (cmd != null) {
			m_cmds.put(cmd.getHeader().getCorrelationId(), cmd);
		}
	}

	@Override
	public void resultReceived(PullMessageResultCommand result) {
		if (result != null) {
			PullMessageCommand pullMessageCommand = null;
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
	public void remove(PullMessageCommand cmd) {
		if (cmd != null) {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		}
	}
}
