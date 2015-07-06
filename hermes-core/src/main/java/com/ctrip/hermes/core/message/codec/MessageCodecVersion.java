package com.ctrip.hermes.core.message.codec;

import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.core.message.codec.internal.MessageCodecBinaryV1Handler;
import com.ctrip.hermes.core.message.codec.internal.MessageCodecBinaryV2Handler;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum MessageCodecVersion {
	BINARY_V1((byte) 1, new MessageCodecBinaryV1Handler()), //
	BINARY_V2((byte) 2, new MessageCodecBinaryV2Handler()), //
	;

	private byte m_version;

	private MessageCodecHandler m_handler;

	private static Map<Byte, MessageCodecVersion> m_versions = new HashMap<>();

	static {
		for (MessageCodecVersion version : MessageCodecVersion.values()) {
			m_versions.put(version.getVersion(), version);
		}
	}

	private MessageCodecVersion(byte version, MessageCodecHandler handler) {
		m_version = version;
		m_handler = handler;
	}

	public byte getVersion() {
		return m_version;
	}

	public MessageCodecHandler getHandler() {
		return m_handler;
	}

	public static MessageCodecVersion valueOf(byte version) {
		return m_versions.get(version);
	}
}
