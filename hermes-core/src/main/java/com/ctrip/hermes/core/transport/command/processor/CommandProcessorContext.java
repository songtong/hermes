package com.ctrip.hermes.core.transport.command.processor;

import io.netty.channel.Channel;

import com.ctrip.hermes.core.transport.command.Command;

public class CommandProcessorContext {

	private Command m_command;

	private Channel m_channel;

	public CommandProcessorContext(Command command, Channel channel) {
		m_command = command;
		m_channel = channel;
	}

	public void write(Command cmd) {
		m_channel.writeAndFlush(cmd);
	}

	public Command getCommand() {
		return m_command;
	}

	public Channel getChannel() {
		return m_channel;
	}

}
