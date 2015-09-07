package com.ctrip.hermes.core.utils;

import io.netty.buffer.ByteBuf;

import java.nio.ByteOrder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Offset;
import com.google.common.base.Charsets;

public class HermesPrimitiveCodec {

	private static final byte NULL = -1;

	private ByteBuf m_buf;

	public HermesPrimitiveCodec(ByteBuf buf) {
		m_buf = buf;
		m_buf.order(ByteOrder.BIG_ENDIAN);
	}

	public void writeBoolean(boolean b) {
		m_buf.writeByte(b ? 1 : 0);
	}

	public boolean readBoolean() {
		return m_buf.readByte() != 0;
	}

	public void writeBytes(byte[] bytes) {
		if (null == bytes) {
			writeNull();
		} else {
			int length = bytes.length;
			m_buf.writeInt(length);
			m_buf.writeBytes(bytes);
		}
	}

	public void writeBytes(ByteBuf buf) {
		if (null == buf) {
			writeNull();
		} else {
			int length = buf.readableBytes();
			m_buf.writeInt(length);
			m_buf.writeBytes(buf);
		}
	}

	public byte[] readBytes() {
		byte firstByte = m_buf.readByte();
		if (firstByte == NULL) {
			return null;
		} else {
			readerIndexBack(m_buf, 1);
			byte[] bytes;
			int length = m_buf.readInt();
			bytes = new byte[length];
			m_buf.readBytes(bytes);
			return bytes;
		}
	}

	private void readerIndexBack(ByteBuf buf, int i) {
		buf.readerIndex(buf.readerIndex() - i);
	}

	public void writeString(String str) {
		if (null == str) {
			writeNull();
		} else {
			int length = str.getBytes(Charsets.UTF_8).length;
			m_buf.writeInt(length);
			m_buf.writeBytes(str.getBytes(Charsets.UTF_8));
		}
	}

	public String readString() {
		byte firstByte = m_buf.readByte();
		if (NULL == firstByte) {
			return null;
		} else {
			readerIndexBack(m_buf, 1);
			int strLen = m_buf.readInt();
			byte[] strBytes = new byte[strLen];
			m_buf.readBytes(strBytes);
			return new String(strBytes, Charsets.UTF_8);
		}
	}

	public void writeInt(int i) {
		m_buf.writeInt(i);
	}

	public int readInt() {
		return m_buf.readInt();
	}

	public void writeChar(char c) {
		m_buf.writeChar(c);
	}

	public char readChar() {
		return m_buf.readChar();
	}

	public void writeLong(long v) {
		m_buf.writeLong(v);
	}

	public long readLong() {
		return m_buf.readLong();
	}

	public Offset readOffset() {
		byte firstByte = m_buf.readByte();
		if (NULL == firstByte) {
			return null;
		} else {
			readerIndexBack(m_buf, 1);
			long pOff = m_buf.readLong();
			long npOff = m_buf.readLong();
			Date rDate = new Date(m_buf.readLong());
			long rOff = m_buf.readLong();
			return new Offset(pOff, npOff, new Pair<Date, Long>(rDate, rOff));
		}
	}

	public void writeOffset(Offset offset) {
		if (offset == null) {
			writeNull();
		} else {
			m_buf.writeLong(offset.getPriorityOffset());
			m_buf.writeLong(offset.getNonPriorityOffset());
			m_buf.writeLong(offset.getResendOffset().getKey().getTime());
			m_buf.writeLong(offset.getResendOffset().getValue());
		}
	}

	public void writeStringStringMap(Map<String, String> map) {
		if (null == map) {
			writeNull();
		} else {
			m_buf.writeInt(map.size());

			if (map.size() > 0) {
				for (Map.Entry<String, String> entry : map.entrySet()) {
					writeString(entry.getKey());
					writeString(entry.getValue());
				}
			}
		}
	}

	public void writeLongIntMap(Map<Long, Integer> map) {
		if (null == map) {
			writeNull();
		} else {
			m_buf.writeInt(map.size());

			if (map.size() > 0) {
				for (Map.Entry<Long, Integer> entry : map.entrySet()) {
					writeLong(entry.getKey());
					writeInt(entry.getValue());
				}
			}
		}
	}

	public void writeIntBooleanMap(Map<Integer, Boolean> map) {
		if (null == map) {
			writeNull();
		} else {
			m_buf.writeInt(map.size());

			if (map.size() > 0) {
				for (Map.Entry<Integer, Boolean> entry : map.entrySet()) {
					writeInt(entry.getKey());
					writeBoolean(entry.getValue());
				}
			}
		}
	}

	public Map<String, String> readStringStringMap() {
		byte firstByte = m_buf.readByte();
		if (NULL == firstByte) {
			return null;
		} else {
			readerIndexBack(m_buf, 1);
			int length = m_buf.readInt();
			Map<String, String> result = new HashMap<String, String>();
			if (length > 0) {
				for (int i = 0; i < length; i++) {
					result.put(readString(), readString());
				}
			}
			return result;
		}
	}

	public Map<Long, Integer> readLongIntMap() {
		byte firstByte = m_buf.readByte();
		if (NULL == firstByte) {
			return null;
		} else {
			readerIndexBack(m_buf, 1);
			int length = m_buf.readInt();
			Map<Long, Integer> result = new HashMap<Long, Integer>();
			if (length > 0) {
				for (int i = 0; i < length; i++) {
					result.put(readLong(), readInt());
				}
			}
			return result;
		}
	}

	public Map<Integer, Boolean> readIntBooleanMap() {
		byte firstByte = m_buf.readByte();
		if (NULL == firstByte) {
			return null;
		} else {
			readerIndexBack(m_buf, 1);
			int length = m_buf.readInt();
			Map<Integer, Boolean> result = new HashMap<Integer, Boolean>();
			if (length > 0) {
				for (int i = 0; i < length; i++) {
					result.put(readInt(), readBoolean());
				}
			}
			return result;
		}
	}

	public void writeNull() {
		m_buf.writeByte(NULL);
	}

	public ByteBuf getBuf() {
		return m_buf;
	}

}
