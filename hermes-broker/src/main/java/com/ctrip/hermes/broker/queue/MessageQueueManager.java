package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.command.AckMessageCommand.AckContext;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageQueueManager {

	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(Tpp tpp, MessageBatchWithRawData data, Lease lease);

	public MessageQueueCursor getCursor(Tpg tpg, Lease lease, String sessionId);

	public void stop();

	void delivered(Tpp tpp, String groupId, boolean resend, List<MessageMeta> msgMetas);

	void acked(Tpp tpp, String groupId, boolean resend, List<AckContext> ackContexts);

	void nacked(Tpp tpp, String groupId, boolean resend, List<AckContext> nackContexts);

}
