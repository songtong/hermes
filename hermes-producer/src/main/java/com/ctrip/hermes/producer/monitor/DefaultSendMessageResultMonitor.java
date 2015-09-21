package com.ctrip.hermes.producer.monitor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.producer.status.ProducerStatusMonitor;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultTransaction;
import com.dianping.cat.message.spi.MessageTree;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = SendMessageResultMonitor.class)
public class DefaultSendMessageResultMonitor implements SendMessageResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultSendMessageResultMonitor.class);

	private Map<Long, Pair<SendMessageCommand, SettableFuture<Boolean>>> m_cmds = new ConcurrentHashMap<>();

	private ReentrantLock m_lock = new ReentrantLock();

	@Override
	public Future<Boolean> monitor(SendMessageCommand cmd) {
		m_lock.lock();
		try {
			SettableFuture<Boolean> future = SettableFuture.create();
			m_cmds.put(cmd.getHeader().getCorrelationId(), new Pair<SendMessageCommand, SettableFuture<Boolean>>(cmd,
			      future));
			return future;
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void resultReceived(SendMessageResultCommand result) {
		if (result != null) {
			Pair<SendMessageCommand, SettableFuture<Boolean>> pair = null;
			m_lock.lock();
			try {
				pair = m_cmds.remove(result.getHeader().getCorrelationId());
			} finally {
				m_lock.unlock();
			}
			if (pair != null) {
				try {
					SendMessageCommand sendMessageCommand = pair.getKey();
					SettableFuture<Boolean> future = pair.getValue();

					ProducerStatusMonitor.INSTANCE.brokerResultReceived(sendMessageCommand.getTopic(),
					      sendMessageCommand.getPartition(), sendMessageCommand.getMessageCount());

					if (isResultSuccess(result)) {
						future.set(true);
					} else {
						future.set(false);
					}
					sendMessageCommand.onResultReceived(result);
					tracking(sendMessageCommand, true);
				} catch (Exception e) {
					log.warn("Exception occurred while calling resultReceived", e);
				}

			}
		}
	}

	private boolean isResultSuccess(SendMessageResultCommand result) {
		Map<Integer, Boolean> successes = result.getSuccesses();
		for (Boolean success : successes.values()) {
			if (!success) {
				return false;
			}
		}

		return true;
	}

	private void tracking(SendMessageCommand sendMessageCommand, boolean success) {
		String status = success ? Transaction.SUCCESS : "Timeout";

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

				Transaction elapseT = Cat.newTransaction("Message.Produce.Elapse", msg.getTopic());
				if (elapseT instanceof DefaultTransaction) {
					((DefaultTransaction) elapseT).setDurationStart(msg.getBornTimeNano());
					elapseT.addData("command.message.count", sendMessageCommand.getMessageCount());
				}
				elapseT.setStatus(status);
				elapseT.complete();

				t.setStatus(status);
				t.complete();
			}
		}
	}

	@Override
	public void cancel(SendMessageCommand cmd) {
		m_lock.lock();
		try {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		} finally {
			m_lock.unlock();
		}
	}

}
