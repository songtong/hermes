package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager.Operation;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueue {

	ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(boolean isPriority, MessageBatchWithRawData batch,
	      Lease lease);

	MessageQueueCursor getCursor(String groupId, Lease lease);

	Offset findLatestOffset(String groupId);

	void nack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas);

	void ack(boolean resend, boolean isPriority, String groupId, long msgSeq);

	void stop();

	void checkHolders();

	boolean offer(Operation operation);
}
