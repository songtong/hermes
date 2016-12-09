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
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.status.BrokerStatusMonitor;
import com.ctrip.hermes.core.bo.SendMessageResult;
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
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v6.SendMessageAckCommandV6;
import com.ctrip.hermes.core.transport.command.v6.SendMessageCommandV6;
import com.ctrip.hermes.core.transport.command.v6.SendMessageResultCommandV6;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
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
public class SendMessageCommandProcessorV6 implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(SendMessageCommandProcessorV6.class);

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
		return Arrays.asList(CommandType.MESSAGE_SEND_V6);
	}

	@Override
	public void process(final CommandProcessorContext ctx) {
		SendMessageCommandV6 reqCmd = (SendMessageCommandV6) ctx.getCommand();
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

				final SendMessageResultCommandV6 result = new SendMessageResultCommandV6(reqCmd.getMessageCount());
				result.correlate(reqCmd);

				FutureCallback<Map<Integer, SendMessageResult>> completionCallback = new AppendMessageCompletionCallback(
				      result, ctx, topic, partition);

				for (Map.Entry<Integer, MessageBatchWithRawData> entry : rawBatches.entrySet()) {
					MessageBatchWithRawData batch = entry.getValue();
					try {
						ListenableFuture<Map<Integer, SendMessageResult>> future = m_queueManager.appendMessageAsync(topic,
						      partition, entry.getKey() == 0 ? true : false, batch,
						      m_systemClockService.now() + reqCmd.getTimeout());

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

	private void logReqToCat(SendMessageCommandV6 reqCmd) {
		CatUtil.logEventPeriodically(CatConstants.TYPE_SEND_CMD + reqCmd.getHeader().getType().getVersion(),
		      reqCmd.getTopic() + "-" + reqCmd.getPartition());
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

	private class AppendMessageCompletionCallback implements FutureCallback<Map<Integer, SendMessageResult>> {
		private SendMessageResultCommandV6 m_result;

		private CommandProcessorContext m_ctx;

		private AtomicBoolean m_written = new AtomicBoolean(false);

		private String m_topic;

		private int m_partition;

		private long m_start;

		public AppendMessageCompletionCallback(SendMessageResultCommandV6 result, CommandProcessorContext ctx,
		      String topic, int partition) {
			m_result = result;
			m_ctx = ctx;
			m_topic = topic;
			m_partition = partition;
			m_start = System.currentTimeMillis();
		}

		@Override
		public void onSuccess(Map<Integer, SendMessageResult> results) {
			m_result.addResults(results);

			if (m_result.isAllResultsSet()) {
				try {
					if (m_written.compareAndSet(false, true)) {
						logToCatIfHasError(m_result);
						ChannelUtils.writeAndFlush(m_ctx.getChannel(), m_result);

						logElapse();
					}
				} finally {
					m_ctx.getCommand().release();
				}
			}
		}

		private void logElapse() {
			CatUtil.logElapse(CatConstants.TYPE_MESSAGE_BROKER_PRODUCE_DB + findDb(m_topic, m_partition), m_topic,
			      m_start, m_result.getResults().size(), null, Transaction.SUCCESS);
		}

		private String findDb(String topic, int partition) {
			Partition p = m_metaService.findPartitionByTopicAndPartition(topic, partition);
			return p.getWriteDatasource();
		}

		private void logToCatIfHasError(SendMessageResultCommandV6 resultCmd) {
			int failCount = 0;
			for (SendMessageResult result : resultCmd.getResults().values()) {
				if (!result.isSuccess()) {
					failCount++;
				}
			}
			if (failCount > 0) {
				Cat.logEvent(CatConstants.TYPE_MESSAGE_PRODUCE_ERROR, m_topic, Event.SUCCESS, "*count=" + failCount);
			}
		}

		@Override
		public void onFailure(Throwable t) {
			// TODO
		}
	}

	private void writeAck(CommandProcessorContext ctx, String topic, int partition, boolean success) {
		SendMessageCommandV6 req = (SendMessageCommandV6) ctx.getCommand();

		SendMessageAckCommandV6 ack = new SendMessageAckCommandV6();
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
