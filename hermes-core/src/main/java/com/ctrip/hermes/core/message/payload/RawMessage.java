package com.ctrip.hermes.core.message.payload;

public class RawMessage {

	private byte[] m_undecodedMessage;

	public RawMessage(byte[] undecodedMessage) {
		m_undecodedMessage = undecodedMessage;
	}

	public byte[] getUndecodedMessage() {
		return m_undecodedMessage;
	}

}
