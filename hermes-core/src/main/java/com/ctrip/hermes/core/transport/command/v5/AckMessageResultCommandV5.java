package com.ctrip.hermes.core.transport.command.v5;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckMessageResultCommandV5 extends AbstractCommand {

	private static final long serialVersionUID = -2462726426306841225L;

	private boolean m_success = false;

	private Endpoint m_newEndpoint;

	public AckMessageResultCommandV5() {
		super(CommandType.RESULT_ACK_MESSAGE_V5, 5);
	}

	public void setSuccess(boolean success) {
		m_success = success;
	}

	public boolean isSuccess() {
		return m_success;
	}

	public Endpoint getNewEndpoint() {
		return m_newEndpoint;
	}

	public void setNewEndpoint(Endpoint newEndpoint) {
		m_newEndpoint = newEndpoint;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_success = codec.readBoolean();
		m_newEndpoint = codec.readEndpoint();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeBoolean(m_success);
		codec.writeEndpoint(m_newEndpoint);
	}

}
