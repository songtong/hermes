package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager.Operation;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueue {

	String getTopic();

	int getPartition();

	ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(boolean isPriority, MessageBatchWithRawData batch,
	      long expireTime);

	MessageQueueCursor getCursor(String groupId, Lease lease, Offset offset);

	Offset findLatestConsumerOffset(String groupId);

	Offset findMessageOffsetByTime(long time);

	TppConsumerMessageBatch findMessagesByOffsets(boolean isPriority, List<Long> offsets);

	void nack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas);

	void ack(boolean resend, boolean isPriority, String groupId, long msgSeq);

	void stop();

	void checkHolders();

	boolean offerAckHolderOp(Operation operation);

	boolean offerAckMessagesTask(AckMessagesTask task);

	boolean flush(ExecutorService executor, int batchSize);

}
