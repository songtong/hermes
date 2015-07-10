package com.ctrip.hermes.producer.sender;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageSender {

	Future<SendResult> send(ProducerMessage<?> msg);

	void resend(ProducerMessage<?> msg, SettableFuture<SendResult> future);
}
