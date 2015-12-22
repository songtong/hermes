package com.ctrip.hermes.broker.queue;

import java.util.Map;

import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueueFlusher {
	boolean hasUnflushedMessages();

	boolean startFlush();

	void flush(int batchSize);

	void finishFlush();

	ListenableFuture<Map<Integer, Boolean>> append(boolean isPriority, MessageBatchWithRawData batch, long expireTime);
}
