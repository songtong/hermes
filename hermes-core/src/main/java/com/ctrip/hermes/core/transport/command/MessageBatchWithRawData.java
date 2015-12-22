package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class MessageBatchWithRawData {
	private String m_topic;

	private List<Integer> m_msgSeqs;

	private ByteBuf m_rawData;

	private List<PartialDecodedMessage> m_msgs;

	public MessageBatchWithRawData(String topic, List<Integer> msgSeqs, ByteBuf rawData) {
		m_topic = topic;
		m_msgSeqs = msgSeqs;
		m_rawData = rawData;
	}

	public String getTopic() {
		return m_topic;
	}

	public List<Integer> getMsgSeqs() {
		return m_msgSeqs;
	}

	public ByteBuf getRawData() {
		return m_rawData.duplicate();
	}

	public List<PartialDecodedMessage> getMessages() {

		if (m_msgs == null) {
			synchronized (this) {
				if (m_msgs == null) {
					m_msgs = new ArrayList<PartialDecodedMessage>();

					ByteBuf tmpBuf = m_rawData.duplicate();
					MessageCodec messageCodec = PlexusComponentLocator.lookup(MessageCodec.class);

					while (tmpBuf.readableBytes() > 0) {
						m_msgs.add(messageCodec.decodePartial(tmpBuf));
					}

				}
			}
		}

		return m_msgs;
	}
}