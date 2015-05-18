package com.ctrip.hermes.broker.queue;

import java.util.Map;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueueDumper {

	void start();

	void submit(SettableFuture<Map<Integer, Boolean>> future, MessageBatchWithRawData batch, boolean isPriority);

	Lease getLease();
}
