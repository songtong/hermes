package com.ctrip.hermes.producer.monitor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.v5.SendMessageCommandV5;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.status.ProducerStatusMonitor;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = SendMessageResultMonitor.class)
public class DefaultSendMessageResultMonitor implements SendMessageResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultSendMessageResultMonitor.class);

	private Map<Long, Pair<SendMessageCommandV5, SettableFuture<Boolean>>> m_cmds = new ConcurrentHashMap<>();

	private ReentrantLock m_lock = new ReentrantLock();

	@Inject
	private ProducerConfig m_config;

	@Override
	public Future<Boolean> monitor(SendMessageCommandV5 cmd) {
		m_lock.lock();
		try {
			SettableFuture<Boolean> future = SettableFuture.create();
			m_cmds.put(cmd.getHeader().getCorrelationId(), new Pair<SendMessageCommandV5, SettableFuture<Boolean>>(cmd,
			      future));
			return future;
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void resultReceived(SendMessageResultCommand result) {
		if (result != null) {
			Pair<SendMessageCommandV5, SettableFuture<Boolean>> pair = null;
			m_lock.lock();
			try {
				pair = m_cmds.remove(result.getHeader().getCorrelationId());
			} finally {
				m_lock.unlock();
			}
			if (pair != null) {
				try {
					SendMessageCommandV5 sendMessageCommand = pair.getKey();
					SettableFuture<Boolean> future = pair.getValue();

					ProducerStatusMonitor.INSTANCE.brokerResultReceived(sendMessageCommand.getTopic(),
					      sendMessageCommand.getPartition(), sendMessageCommand.getMessageCount());

					if (isResultSuccess(result)) {
						future.set(true);
						sendMessageCommand.onResultReceived(result);
					} else {
						future.set(false);
					}
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

	private void tracking(SendMessageCommandV5 sendMessageCommand, boolean success) {
		if (!success || m_config.isCatEnabled()) {
			String status = success ? Transaction.SUCCESS : "Timeout";
			Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_ACKED, sendMessageCommand.getTopic());
			for (List<ProducerMessage<?>> msgs : sendMessageCommand.getProducerMessages()) {
				for (ProducerMessage<?> msg : msgs) {
					MessageTree tree = Cat.getManager().getThreadLocalMessageTree();

					String msgId = msg.getDurableSysProperty(CatConstants.SERVER_MESSAGE_ID);
					String parentMsgId = msg.getDurableSysProperty(CatConstants.CURRENT_MESSAGE_ID);
					String rootMsgId = msg.getDurableSysProperty(CatConstants.ROOT_MESSAGE_ID);

					tree.setMessageId(msgId);
					tree.setParentMessageId(parentMsgId);
					tree.setRootMessageId(rootMsgId);

					long durtion = System.currentTimeMillis() - msg.getBornTime();
					String type = durtion > 2000L ? CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE_LARGE
					      : CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE;
					CatUtil.logElapse(type, msg.getTopic(), msg.getBornTime(), 1,
					      Arrays.asList(new Pair<String, String>("key", msg.getKey())), status);
				}
			}
			t.addData("*count", sendMessageCommand.getMessageCount());
			t.setStatus(status);
			t.complete();
		}
	}

	@Override
	public void cancel(SendMessageCommandV5 cmd) {
		m_lock.lock();
		try {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		} finally {
			m_lock.unlock();
		}
	}

}
