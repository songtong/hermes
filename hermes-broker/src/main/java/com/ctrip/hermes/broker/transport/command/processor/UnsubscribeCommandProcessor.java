package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.transport.transmitter.MessageTransmitter;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.UnsubscribeCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

public class UnsubscribeCommandProcessor implements CommandProcessor {

	@Inject
	private MessageTransmitter m_transmitter;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.UNSUBSCRIBE);
	}

	@Override
	public void process(CommandProcessorContext ctx) {

		UnsubscribeCommand reqCmd = (UnsubscribeCommand) ctx.getCommand();
		// TODO
		System.out.println("Unsubscribe..." + reqCmd);

		long correlationId = reqCmd.getHeader().getCorrelationId();
		m_transmitter.deregisterDestination(correlationId, ctx.getChannel());
	}

}
