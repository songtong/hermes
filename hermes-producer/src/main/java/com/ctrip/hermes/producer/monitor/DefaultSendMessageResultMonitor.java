package com.ctrip.hermes.producer.monitor;

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
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.sender.MessageSender;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = SendMessageResultMonitor.class)
public class DefaultSendMessageResultMonitor implements SendMessageResultMonitor, Initializable {
	private static final Logger log = LoggerFactory.getLogger(DefaultSendMessageResultMonitor.class);

	@Inject
	private ProducerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	private Map<Long, SendMessageCommand> m_cmds = new ConcurrentHashMap<Long, SendMessageCommand>();

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
	public void resultReceived(SendMessageResultCommand result) {
		if (result != null) {
			SendMessageCommand sendMessageCommand = null;
			m_lock.lock();
			try {
				sendMessageCommand = m_cmds.remove(result.getHeader().getCorrelationId());
			} finally {
				m_lock.unlock();
			}
			if (sendMessageCommand != null) {
				try {
					sendMessageCommand.onResultReceived(result);
					tracking(sendMessageCommand, true);
				} catch (Exception e) {
					log.warn("Exception occurred while calling resultReceived", e);
				}

			}
		}
	}

	private void tracking(SendMessageCommand sendMessageCommand, boolean success) {
		for (List<ProducerMessage<?>> msgs : sendMessageCommand.getProducerMessages()) {
			for (ProducerMessage<?> msg : msgs) {
				Transaction t = Cat.newTransaction("Message.Produce.Acked", msg.getTopic());
				MessageTree tree = Cat.getManager().getThreadLocalMessageTree();

				String msgId = msg.getDurableSysProperty(CatConstants.SERVER_MESSAGE_ID);
				String parentMsgId = msg.getDurableSysProperty(CatConstants.CURRENT_MESSAGE_ID);
				String rootMsgId = msg.getDurableSysProperty(CatConstants.ROOT_MESSAGE_ID);

				tree.setMessageId(msgId);
				tree.setParentMessageId(parentMsgId);
				tree.setRootMessageId(rootMsgId);

				t.setStatus(success ? Transaction.SUCCESS : "Timeout");
				t.complete();
			}
		}
	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(
		      HermesThreadFactory.create("SendMessageResultMonitor-HouseKeeper", true)).scheduleWithFixedDelay(
		      new Runnable() {

			      @Override
			      public void run() {
				      try {
					      scanAndResendTimeoutCommands();
				      } catch (Exception e) {
					      // ignore
					      if (log.isDebugEnabled()) {
						      log.debug("Exception occurred while running SendMessageResultMonitor-HouseKeeper", e);
					      }
				      }
			      }

		      }, 5, 5, TimeUnit.SECONDS);
	}

	protected void scanAndResendTimeoutCommands() {
		List<SendMessageCommand> timeoutCmds = scanTimeoutCommands();

		if (!timeoutCmds.isEmpty()) {
			resend(timeoutCmds);
		}
	}

	protected List<SendMessageCommand> scanTimeoutCommands() {
		List<SendMessageCommand> timeoutCmds = new LinkedList<SendMessageCommand>();
		m_lock.lock();
		try {
			for (Map.Entry<Long, SendMessageCommand> entry : m_cmds.entrySet()) {
				SendMessageCommand cmd = entry.getValue();
				Long correlationId = entry.getKey();
				if (cmd.isExpired(m_systemClockService.now(), m_config.getSendMessageReadResultTimeoutMillis())) {
					timeoutCmds.add(m_cmds.remove(correlationId));
				}
			}

		} finally {
			m_lock.unlock();
		}
		return timeoutCmds;
	}

	protected void resend(List<SendMessageCommand> timeoutCmds) {
		for (SendMessageCommand cmd : timeoutCmds) {
			List<Pair<ProducerMessage<?>, SettableFuture<SendResult>>> msgFuturePairs = cmd
			      .getProducerMessageFuturePairs();
			for (Pair<ProducerMessage<?>, SettableFuture<SendResult>> pair : msgFuturePairs) {
				MessageSender messageSender = PlexusComponentLocator.lookup(MessageSender.class);
				if (messageSender != null) {
					messageSender.resend(pair.getKey(), pair.getValue());
				}
			}
		}
	}
}
