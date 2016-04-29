package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.status.BrokerStatusMonitor;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.FileBizLogger;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.meta.entity.Storage;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultTransaction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageCommandProcessor implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(SendMessageCommandProcessor.class);

	@Inject
	private FileBizLogger m_bizLogger;

	@Inject
	private MessageQueueManager m_queueManager;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfig m_config;

	@Inject
	private MetaService m_metaService;

	@Inject
	private SystemClockService m_systemClockService;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_SEND);
	}

	@Override
	public void process(final CommandProcessorContext ctx) {
		SendMessageCommand reqCmd = (SendMessageCommand) ctx.getCommand();

		Cat.logEvent("Hermes.SendMessage.Request", reqCmd.getTopic() + "-" + reqCmd.getPartition());
		Lease lease = m_leaseContainer.acquireLease(reqCmd.getTopic(), reqCmd.getPartition(), m_config.getSessionId());

		if (m_metaService.findTopicByName(reqCmd.getTopic()) != null) {
			if (lease != null) {
				if (log.isDebugEnabled()) {
					log.debug("Send message reqeust arrived(topic={}, partition={}, msgCount={})", reqCmd.getTopic(),
					      reqCmd.getPartition(), reqCmd.getMessageCount());
				}

				// FIXME if dumper's queue is full, reject it.
				writeAck(ctx, true);

				Map<Integer, MessageBatchWithRawData> rawBatches = reqCmd.getMessageRawDataBatches();

				bizLog(ctx, rawBatches, reqCmd.getPartition());

				final SendMessageResultCommand result = new SendMessageResultCommand(reqCmd.getMessageCount());
				result.correlate(reqCmd);

				FutureCallback<Map<Integer, Boolean>> completionCallback = new AppendMessageCompletionCallback(result, ctx,
				      reqCmd.getTopic(), reqCmd.getPartition());

				for (Map.Entry<Integer, MessageBatchWithRawData> entry : rawBatches.entrySet()) {
					MessageBatchWithRawData batch = entry.getValue();
					try {
						ListenableFuture<Map<Integer, Boolean>> future = m_queueManager.appendMessageAsync(reqCmd.getTopic(),
						      reqCmd.getPartition(), entry.getKey() == 0 ? true : false, batch,
						      m_systemClockService.now() + 10 * 1000L);

						if (future != null) {
							Futures.addCallback(future, completionCallback);
						}
					} catch (Exception e) {
						log.error("Failed to append messages async.", e);
					}
				}

				return;

			} else {
				if (log.isDebugEnabled()) {
					log.debug("No broker lease to handle client send message reqeust(topic={}, partition={})",
					      reqCmd.getTopic(), reqCmd.getPartition());
				}
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Topic {} not found", reqCmd.getTopic());
			}
		}

		writeAck(ctx, false);
		reqCmd.release();
	}

	private void bizLog(CommandProcessorContext ctx, Map<Integer, MessageBatchWithRawData> rawBatches, int partition) {
		for (Entry<Integer, MessageBatchWithRawData> entry : rawBatches.entrySet()) {
			MessageBatchWithRawData batch = entry.getValue();
			if (!Storage.KAFKA.equals(m_metaService.findTopicByName(batch.getTopic()).getStorageType())) {
				List<PartialDecodedMessage> msgs = batch.getMessages();
				BrokerStatusMonitor.INSTANCE.msgReceived(batch.getTopic(), partition, ctx.getRemoteIp(), batch.getRawData()
				      .readableBytes(), msgs.size());
				for (PartialDecodedMessage msg : msgs) {
					BizEvent event = new BizEvent("Message.Received");
					event.addData("topic", m_metaService.findTopicByName(batch.getTopic()).getId());
					event.addData("partition", partition);
					event.addData("priority", entry.getKey());
					event.addData("producerIp", ctx.getRemoteIp());
					event.addData("bornTime", new Date(msg.getBornTime()));
					event.addData("refKey", msg.getKey());

					m_bizLogger.log(event);
				}
			}
		}
	}

	private static class AppendMessageCompletionCallback implements FutureCallback<Map<Integer, Boolean>> {
		private SendMessageResultCommand m_result;

		private CommandProcessorContext m_ctx;

		private AtomicBoolean m_written = new AtomicBoolean(false);

		private String m_topic;

		private int m_partition;

		private long m_start;

		public AppendMessageCompletionCallback(SendMessageResultCommand result, CommandProcessorContext ctx,
		      String topic, int partition) {
			m_result = result;
			m_ctx = ctx;
			m_topic = topic;
			m_partition = partition;
			m_start = System.nanoTime();
		}

		@Override
		public void onSuccess(Map<Integer, Boolean> results) {
			m_result.addResults(results);

			if (m_result.isAllResultsSet()) {
				try {
					if (m_written.compareAndSet(false, true)) {
						logToCatIfHasError(m_result);
						m_result.getHeader().addProperty("createTime", Long.toString(System.currentTimeMillis()));
						m_ctx.write(m_result);

						logElapse();
					}
				} finally {
					m_ctx.getCommand().release();
				}
			}
		}

		private void logElapse() {
			Transaction elapseT = Cat.newTransaction(CatConstants.TYPE_MESSAGE_BROKER_PRODUCE_ELAPSE, m_topic + "-"
			      + m_partition);
			if (elapseT instanceof DefaultTransaction) {
				((DefaultTransaction) elapseT).setDurationStart(m_start);
				elapseT.addData("*count", m_result.getSuccesses().size());
			}
			elapseT.setStatus(Transaction.SUCCESS);
			elapseT.complete();
		}

		private void logToCatIfHasError(SendMessageResultCommand resultCmd) {
			for (Boolean sendSuccess : resultCmd.getSuccesses().values()) {
				if (!sendSuccess) {
					Cat.logEvent(CatConstants.TYPE_MESSAGE_PRODUCE_ERROR, m_topic, Event.SUCCESS, "");
				}
			}
		}

		@Override
		public void onFailure(Throwable t) {
			// TODO
		}
	}

	private void writeAck(CommandProcessorContext ctx, boolean success) {
		SendMessageCommand req = (SendMessageCommand) ctx.getCommand();

		SendMessageAckCommand ack = new SendMessageAckCommand();
		ack.correlate(req);
		ack.setSuccess(success);
		ack.getHeader().addProperty("createTime", Long.toString(System.currentTimeMillis()));
		ChannelUtils.writeAndFlush(ctx.getChannel(), ack);
	}

}
