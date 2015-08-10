package com.ctrip.hermes.producer.sender;

import java.util.List;
import java.util.concurrent.Future;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MessageSender {

	Future<SendResult> send(ProducerMessage<?> msg);

	void resend(List<SendMessageCommand> timeoutCmds);
}
