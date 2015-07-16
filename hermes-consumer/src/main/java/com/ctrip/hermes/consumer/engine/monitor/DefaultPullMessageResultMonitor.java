package com.ctrip.hermes.consumer.engine.monitor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = PullMessageResultMonitor.class)
public class DefaultPullMessageResultMonitor implements PullMessageResultMonitor, Initializable {
	private static final Logger log = LoggerFactory.getLogger(DefaultPullMessageResultMonitor.class);

	@Inject
	private SystemClockService m_systemClockService;

	private Map<Long, PullMessageCommand> m_cmds = new ConcurrentHashMap<Long, PullMessageCommand>();

	private ReentrantLock m_lock = new ReentrantLock();

	@Override
	public void monitor(PullMessageCommand cmd) {
		if (cmd != null) {
			m_lock.lock();
			try {
				m_cmds.put(cmd.getHeader().getCorrelationId(), cmd);
			} finally {
				m_lock.unlock();
			}
		}
	}

	@Override
	public void resultReceived(PullMessageResultCommand result) {
		if (result != null) {
			PullMessageCommand pullMessageCommand = null;
			m_lock.lock();
			try {
				pullMessageCommand = m_cmds.remove(result.getHeader().getCorrelationId());
			} finally {
				m_lock.unlock();
			}
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
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(
		      HermesThreadFactory.create("PullMessageResultMonitor-HouseKeeper", true)).scheduleWithFixedDelay(
		      new Runnable() {

			      @Override
			      public void run() {
				      try {
					      List<PullMessageCommand> timeoutCmds = new LinkedList<PullMessageCommand>();

					      m_lock.lock();
					      try {
						      for (Map.Entry<Long, PullMessageCommand> entry : m_cmds.entrySet()) {
							      PullMessageCommand cmd = entry.getValue();
							      Long correlationId = entry.getKey();
							      if (cmd.getExpireTime() + 4000L < m_systemClockService.now()) {
								      timeoutCmds.add(m_cmds.remove(correlationId));
							      }
						      }

					      } finally {
						      m_lock.unlock();
					      }

					      for (PullMessageCommand timeoutCmd : timeoutCmds) {
						      if (log.isDebugEnabled()) {
							      log.debug(
							            "No result received for PullMessageCommand(correlationId={}) until timeout, will cancel waiting automatically",
							            timeoutCmd.getHeader().getCorrelationId());
						      }
						      timeoutCmd.onTimeout();
					      }
				      } catch (Exception e) {
					      // ignore
					      if (log.isDebugEnabled()) {
						      log.debug("Exception occurred while running PullMessageResultMonitor-HouseKeeper", e);
					      }
				      }
			      }
		      }, 5, 5, TimeUnit.SECONDS);
	}
}
