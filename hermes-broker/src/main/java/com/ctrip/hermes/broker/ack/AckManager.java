package com.ctrip.hermes.broker.ack;

import java.util.List;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.command.AckMessageCommand.AckContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface AckManager {

	void delivered(Tpp tpp, String groupId, boolean resend, List<MessageMeta> msgMetas);

	void acked(Tpp tpp, String groupId, boolean resend, List<AckContext> ackContexts);

	void nacked(Tpp tpp, String groupId, boolean resend, List<AckContext> nackContexts);

	void stop();

}
