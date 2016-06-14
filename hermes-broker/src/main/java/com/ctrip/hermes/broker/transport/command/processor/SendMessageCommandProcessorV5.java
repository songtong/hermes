package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

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
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.SendMessageAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.SendMessageCommandV5;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Storage;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageCommandProcessorV5 implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(SendMessageCommandProcessorV5.class);

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

	private AtomicLong m_lastLogSendReqToCatTime = new AtomicLong(0);

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_SEND_V5);
	}

	@Override
	public void process(final CommandProcessorContext ctx) {
		SendMessageCommandV5 reqCmd = (SendMessageCommandV5) ctx.getCommand();
		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();

		logReqToCat(reqCmd);

		Lease lease = m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId());

		if (m_metaService.findTopicByName(topic) != null) {
			if (lease != null) {
				// FIXME if dumper's queue is full, reject it.
				writeAck(ctx, topic, partition, true);

				Map<Integer, MessageBatchWithRawData> rawBatches = reqCmd.getMessageRawDataBatches();

				bizLog(ctx, rawBatches, partition);

				final SendMessageResultCommand result = new SendMessageResultCommand(reqCmd.getMessageCount());
				result.correlate(reqCmd);

				FutureCallback<Map<Integer, Boolean>> completionCallback = new AppendMessageCompletionCallback(result, ctx,
				      topic, partition);

				for (Map.Entry<Integer, MessageBatchWithRawData> entry : rawBatches.entrySet()) {
					MessageBatchWithRawData batch = entry.getValue();
					try {
						ListenableFuture<Map<Integer, Boolean>> future = m_queueManager.appendMessageAsync(topic, partition,
						      entry.getKey() == 0 ? true : false, batch, m_systemClockService.now() + reqCmd.getTimeout());

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
					log.debug("No broker lease to handle client send message reqeust(topic={}, partition={})", topic,
					      partition);
				}
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Topic {} not found", topic);
			}
		}

		writeAck(ctx, topic, partition, false);
		reqCmd.release();
	}

	private void logReqToCat(SendMessageCommandV5 reqCmd) {
		long now = m_systemClockService.now();
		if (now - m_lastLogSendReqToCatTime.get() > 60 * 1000L) {
			Cat.logEvent(CatConstants.TYPE_SEND_CMD + reqCmd.getHeader().getType().getVersion(), reqCmd.getTopic() + "-"
			      + reqCmd.getPartition());

			m_lastLogSendReqToCatTime.set(now);
		}
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
			m_start = System.currentTimeMillis();
		}

		@Override
		public void onSuccess(Map<Integer, Boolean> results) {
			m_result.addResults(results);

			if (m_result.isAllResultsSet()) {
				try {
					if (m_written.compareAndSet(false, true)) {
						logToCatIfHasError(m_result);
						m_result.getHeader().addProperty("createTime", Long.toString(System.currentTimeMillis()));
						ChannelUtils.writeAndFlush(m_ctx.getChannel(), m_result);

						logElapse();
					}
				} finally {
					m_ctx.getCommand().release();
				}
			}
		}

		private void logElapse() {
			CatUtil.logElapse(CatConstants.TYPE_MESSAGE_BROKER_PRODUCE_ELAPSE, m_topic + "-" + m_partition, m_start,
			      m_result.getSuccesses().size(), null, Transaction.SUCCESS);
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

	private void writeAck(CommandProcessorContext ctx, String topic, int partition, boolean success) {
		SendMessageCommandV5 req = (SendMessageCommandV5) ctx.getCommand();

		SendMessageAckCommandV5 ack = new SendMessageAckCommandV5();
		ack.correlate(req);
		ack.setSuccess(success);
		if (!success && m_metaService.findTopicByName(topic) != null) {
			Pair<Endpoint, Long> endpointEntry = m_metaService.findEndpointByTopicAndPartition(topic, partition);
			if (endpointEntry != null) {
				ack.setNewEndpoint(endpointEntry.getKey());
			}
		}
		ChannelUtils.writeAndFlush(ctx.getChannel(), ack);
	}

}
