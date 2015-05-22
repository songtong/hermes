package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.SingleThreaded;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@SingleThreaded
public class SendMessageCommandProcessor implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(SendMessageCommandProcessor.class);

	@Inject
	private MessageQueueManager m_queueManager;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfig m_config;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_SEND);
	}

	@Override
	public void process(final CommandProcessorContext ctx) {
		SendMessageCommand reqCmd = (SendMessageCommand) ctx.getCommand();

		Lease lease = m_leaseContainer.acquireLease(reqCmd.getTopic(), reqCmd.getPartition(), m_config.getSessionId());

		if (lease != null) {
			if (log.isDebugEnabled()) {
				log.debug("Send message reqeust arrived(topic={}, partition={}, msgCount)", reqCmd.getTopic(),
				      reqCmd.getPartition(), reqCmd.getMessageCount());
			}

			writeAck(ctx, true);

			Map<Integer, MessageBatchWithRawData> rawBatches = reqCmd.getMessageRawDataBatches();

			final SendMessageResultCommand result = new SendMessageResultCommand(reqCmd.getMessageCount());
			result.correlate(reqCmd);

			FutureCallback<Map<Integer, Boolean>> completionCallback = new AppendMessageCompletionCallback(result, ctx);

			for (Map.Entry<Integer, MessageBatchWithRawData> entry : rawBatches.entrySet()) {
				MessageBatchWithRawData batch = entry.getValue();
				Tpp tpp = new Tpp(reqCmd.getTopic(), reqCmd.getPartition(), entry.getKey() == 0 ? true : false);
				try {
					ListenableFuture<Map<Integer, Boolean>> future = m_queueManager.appendMessageAsync(tpp, batch, lease);

					Futures.addCallback(future, completionCallback);

				} catch (Exception e) {
					log.error("Failed to append messages async.", e);
				}
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No broker lease to handle client send message reqeust(topic={}, partition={})",
				      reqCmd.getTopic(), reqCmd.getPartition());
			}

			writeAck(ctx, false);
			reqCmd.release();
		}
	}

	private static class AppendMessageCompletionCallback implements FutureCallback<Map<Integer, Boolean>> {
		private SendMessageResultCommand m_result;

		private CommandProcessorContext m_ctx;

		private AtomicBoolean m_written = new AtomicBoolean(false);

		public AppendMessageCompletionCallback(SendMessageResultCommand result, CommandProcessorContext ctx) {
			m_result = result;
			m_ctx = ctx;
		}

		@Override
		public void onSuccess(Map<Integer, Boolean> results) {
			m_result.addResults(results);

			if (m_result.isAllResultsSet()) {
				try {
					if (m_written.compareAndSet(false, true)) {
						m_ctx.write(m_result);
					}
				} finally {
					m_ctx.getCommand().release();
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
		ctx.getChannel().writeCommand(ack);
	}

}
