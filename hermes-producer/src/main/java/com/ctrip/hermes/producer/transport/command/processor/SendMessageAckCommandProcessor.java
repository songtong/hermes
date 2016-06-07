package com.ctrip.hermes.producer.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.SendMessageAckCommandV5;
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
		return Arrays.asList(CommandType.ACK_MESSAGE_SEND_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		SendMessageAckCommandV5 cmd = (SendMessageAckCommandV5) ctx.getCommand();
		m_messageAcceptanceMonitor.received(cmd);
	}

}
