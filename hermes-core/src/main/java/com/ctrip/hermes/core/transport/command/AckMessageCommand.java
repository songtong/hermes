package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class AckMessageCommand extends AbstractCommand {
	private static final long serialVersionUID = 7009170887490443292L;

	// key: tpp, groupId, isResend
	private ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> m_ackMsgSeqs = new ConcurrentHashMap<>();

	// key: tpp, groupId, isResend
	private ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> m_nackMsgSeqs = new ConcurrentHashMap<>();

	public AckMessageCommand() {
		super(CommandType.MESSAGE_ACK);
	}

	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		writeMsgSeqMap(codec, m_ackMsgSeqs);
		writeMsgSeqMap(codec, m_nackMsgSeqs);

	}

	@Override
	protected void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_ackMsgSeqs = readMsgSeqMap(codec);
		m_nackMsgSeqs = readMsgSeqMap(codec);
	}

	private void writeMsgSeqMap(HermesPrimitiveCodec codec,
	      ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> msgSeqMap) {
		if (msgSeqMap == null) {
			codec.writeInt(0);
		} else {
			codec.writeInt(msgSeqMap.size());
			for (Triple<Tpp, String, Boolean> tppgr : msgSeqMap.keySet()) {
				Tpp tpp = tppgr.getFirst();
				codec.writeString(tpp.getTopic());
				codec.writeInt(tpp.getPartition());
				codec.writeInt(tpp.isPriority() ? 0 : 1);
				codec.writeString(tppgr.getMiddle());
				codec.writeBoolean(tppgr.getLast());
			}
			for (Triple<Tpp, String, Boolean> tppgr : msgSeqMap.keySet()) {
				List<AckContext> contexts = msgSeqMap.get(tppgr);

				if (contexts == null || contexts.isEmpty()) {
					codec.writeInt(0);
				} else {
					codec.writeInt(contexts.size());
					for (AckContext context : contexts) {
						codec.writeLong(context.getMsgSeq());
						codec.writeInt(context.getRemainingRetries());
						codec.writeLong(context.getOnMessageStartTimeMillis());
						codec.writeLong(context.getOnMessageEndTimeMillis());
					}
				}
			}
		}

	}

	private ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> readMsgSeqMap(HermesPrimitiveCodec codec) {
		ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> msgSeqMap = new ConcurrentHashMap<>();

		int mapSize = codec.readInt();
		if (mapSize != 0) {
			List<Triple<Tpp, String, Boolean>> tppgrs = new ArrayList<>();
			for (int i = 0; i < mapSize; i++) {
				Tpp tpp = new Tpp(codec.readString(), codec.readInt(), codec.readInt() == 0 ? true : false);
				String groupId = codec.readString();
				boolean resend = codec.readBoolean();
				tppgrs.add(new Triple<>(tpp, groupId, resend));
			}
			for (int i = 0; i < mapSize; i++) {
				Triple<Tpp, String, Boolean> tppgr = tppgrs.get(i);

				int len = codec.readInt();
				if (len == 0) {
					msgSeqMap.put(tppgr, new ArrayList<AckContext>());
				} else {
					msgSeqMap.put(tppgr, new ArrayList<AckContext>(len));
				}

				for (int j = 0; j < len; j++) {
					msgSeqMap.get(tppgr).add(
					      new AckContext(codec.readLong(), codec.readInt(), codec.readLong(), codec.readLong()));
				}
			}
		}

		return msgSeqMap;
	}

	public void addAckMsg(Tpp tpp, String groupId, boolean resend, long msgSeq, int remainingRetries,
	      long onMessageStartTimeMillis, long onMessageEndTimeMillis) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		if (!m_ackMsgSeqs.containsKey(key)) {
			m_ackMsgSeqs.putIfAbsent(key, new ArrayList<AckContext>());
		}
		m_ackMsgSeqs.get(key).add(
		      new AckContext(msgSeq, remainingRetries, onMessageStartTimeMillis, onMessageEndTimeMillis));
	}

	public void addNackMsg(Tpp tpp, String groupId, boolean resend, long msgSeq, int remainingRetries,
	      long onMessageStartTimeMillis, long onMessageEndTimeMillis) {
		Triple<Tpp, String, Boolean> key = new Triple<>(tpp, groupId, resend);
		if (!m_nackMsgSeqs.containsKey(key)) {
			m_nackMsgSeqs.putIfAbsent(key, new ArrayList<AckContext>());
		}
		m_nackMsgSeqs.get(key).add(
		      new AckContext(msgSeq, remainingRetries, onMessageStartTimeMillis, onMessageEndTimeMillis));
	}

	public ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> getAckMsgs() {
		return m_ackMsgSeqs;
	}

	public ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> getNackMsgs() {
		return m_nackMsgSeqs;
	}

	public static class AckContext {
		private long m_msgSeq;

		private int m_remainingRetries;

		private long m_onMessageStartTimeMillis;

		private long m_onMessageEndTimeMillis;

		public AckContext(long msgSeq, int remainingRetries, long onMessageStartTimeMillis, long onMessageEndTimeMillis) {
			m_msgSeq = msgSeq;
			m_remainingRetries = remainingRetries;
			m_onMessageStartTimeMillis = onMessageStartTimeMillis;
			m_onMessageEndTimeMillis = onMessageEndTimeMillis;
		}

		public long getMsgSeq() {
			return m_msgSeq;
		}

		public int getRemainingRetries() {
			return m_remainingRetries;
		}

		public long getOnMessageStartTimeMillis() {
			return m_onMessageStartTimeMillis;
		}

		public long getOnMessageEndTimeMillis() {
			return m_onMessageEndTimeMillis;
		}

		@Override
		public String toString() {
			return "AckContext [m_msgSeq=" + m_msgSeq + ", m_remainingRetries=" + m_remainingRetries
			      + ", m_onMessageStartTimeMillis=" + m_onMessageStartTimeMillis + ", m_onMessageEndTimeMillis="
			      + m_onMessageEndTimeMillis + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (m_msgSeq ^ (m_msgSeq >>> 32));
			result = prime * result + (int) (m_onMessageEndTimeMillis ^ (m_onMessageEndTimeMillis >>> 32));
			result = prime * result + (int) (m_onMessageStartTimeMillis ^ (m_onMessageStartTimeMillis >>> 32));
			result = prime * result + m_remainingRetries;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AckContext other = (AckContext) obj;
			if (m_msgSeq != other.m_msgSeq)
				return false;
			if (m_onMessageEndTimeMillis != other.m_onMessageEndTimeMillis)
				return false;
			if (m_onMessageStartTimeMillis != other.m_onMessageStartTimeMillis)
				return false;
			if (m_remainingRetries != other.m_remainingRetries)
				return false;
			return true;
		}

	}
}
