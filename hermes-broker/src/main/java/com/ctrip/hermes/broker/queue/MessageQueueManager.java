package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

public interface MessageQueueManager {

	public ListenableFuture<Map<Integer, Boolean>> appendMessageAsync(Tpp tpp, MessageBatchWithRawData data, Lease lease);

	public MessageQueueCursor getCursor(Tpg tpg, Lease lease);

	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, Integer>> msgSeqs);

	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq);
}
