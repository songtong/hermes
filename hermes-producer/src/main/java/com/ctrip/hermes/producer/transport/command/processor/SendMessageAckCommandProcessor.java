package com.ctrip.hermes.producer.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.producer.monitor.SendMessageAcceptanceMonitor;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageAckCommandProcessor implements CommandProcessor {

	@Inject
	private SendMessageAcceptanceMonitor m_messageAcceptanceMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.ACK_MESSAGE_SEND);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		SendMessageAckCommand cmd = (SendMessageAckCommand) ctx.getCommand();
		m_messageAcceptanceMonitor.received(cmd.getHeader().getCorrelationId(), cmd.isSuccess());
	}

}
