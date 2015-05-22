package com.ctrip.hermes.producer.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.producer.monitor.SendMessageResultMonitor;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SendMessageResultCommandProcessor implements CommandProcessor {

	@Inject
	private SendMessageResultMonitor m_messageResultMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.RESULT_MESSAGE_SEND);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		SendMessageResultCommand cmd = (SendMessageResultCommand) ctx.getCommand();
		m_messageResultMonitor.resultReceived(cmd);
	}

}
