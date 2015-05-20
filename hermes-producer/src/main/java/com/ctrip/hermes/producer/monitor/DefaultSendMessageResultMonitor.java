package com.ctrip.hermes.producer.monitor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = SendMessageResultMonitor.class)
public class DefaultSendMessageResultMonitor implements SendMessageResultMonitor, Initializable {

	@Inject
	private SystemClockService m_systemClockService;

	private Map<Long, SendMessageCommand> m_cmds = new HashMap<>();

	private ReentrantLock m_lock = new ReentrantLock();

	@Override
	public void monitor(SendMessageCommand cmd) {
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
	public void received(SendMessageResultCommand result) {
		if (result != null) {
			SendMessageCommand sendMessageCommand = null;
			m_lock.lock();
			try {
				sendMessageCommand = m_cmds.remove(result.getHeader().getCorrelationId());
			} finally {
				m_lock.unlock();
			}
			if (sendMessageCommand != null) {
				sendMessageCommand.onResultReceived(result);
			}
		}
	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(
		      HermesThreadFactory.create("SendMessageResultMonitor-HouseKeeper", true)).scheduleAtFixedRate(
		      new Runnable() {

			      @Override
			      public void run() {
				      try {
					      m_lock.lock();
					      try {
						      for (Map.Entry<Long, SendMessageCommand> entry : m_cmds.entrySet()) {
							      SendMessageCommand cmd = entry.getValue();
							      Long correlationId = entry.getKey();
							      if (cmd.getExpireTime() < m_systemClockService.now()) {
								      cmd.onTimeout();
								      m_cmds.remove(correlationId);
							      }
						      }
					      } finally {
						      m_lock.unlock();
					      }
				      } catch (Exception e) {
					      // TODO
				      }
			      }
		      }, 5, 5, TimeUnit.SECONDS);
	}
}
