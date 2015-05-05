package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class UnsubscribeCommand extends AbstractCommand {

	private static final long serialVersionUID = 3599083022933864580L;

	public UnsubscribeCommand() {
		super(CommandType.UNSUBSCRIBE);
	}

	@Override
	public void parse0(ByteBuf buf) {
		// do nothing
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		// do nothing
	}

	@Override
	public String toString() {
		return "UnsubscribeCommand [m_header=" + m_header + "]";
	}

}
