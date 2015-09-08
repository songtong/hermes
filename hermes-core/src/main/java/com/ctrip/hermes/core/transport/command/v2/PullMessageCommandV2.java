package com.ctrip.hermes.core.transport.command.v2;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class PullMessageCommandV2 extends AbstractCommand {

	private static final long serialVersionUID = 6338481458963711634L;

	public static final int PULL_WITH_OFFSET = 1;

	public static final int PULL_WITHOUT_OFFSET = 2;

	private int m_pullType = PULL_WITH_OFFSET;

	private String m_groupId;

	private String m_topic;

	private int m_partition;

	private Offset m_offset;

	private int m_size;

	private long m_expireTime;

	private transient SettableFuture<PullMessageResultCommandV2> m_future;

	public PullMessageCommandV2() {
		this(-1, null, -1, null, null, 0, -1L);
	}

	public PullMessageCommandV2(int type, String topic, int partition, String groupId, Offset offset, int size,
	      long expireTime) {
		super(CommandType.MESSAGE_PULL_V2, 2);
		m_pullType = type;
		m_topic = topic;
		m_partition = partition;
		m_groupId = groupId;
		m_offset = offset;
		m_size = size;
		m_expireTime = expireTime;
	}

	public SettableFuture<PullMessageResultCommandV2> getFuture() {
		return m_future;
	}

	public void setFuture(SettableFuture<PullMessageResultCommandV2> future) {
		m_future = future;
	}

	public int getPullType() {
		return m_pullType;
	}

	public long getExpireTime() {
		return m_expireTime;
	}

	public Offset getOffset() {
		return m_offset;
	}

	public int getSize() {
		return m_size;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public void onResultReceived(PullMessageResultCommandV2 ack) {
		m_future.set(ack);
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		m_pullType = codec.readInt();
		m_topic = codec.readString();
		m_partition = codec.readInt();
		m_groupId = codec.readString();
		m_offset = codec.readOffset();
		m_size = codec.readInt();
		m_expireTime = codec.readLong();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeInt(m_pullType);
		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeString(m_groupId);
		codec.writeOffset(m_offset);
		codec.writeInt(m_size);
		codec.writeLong(m_expireTime);
	}

	@Override
	public String toString() {
		return "PullMessageCommandV2 [m_pullType=" + m_pullType + ", m_groupId=" + m_groupId + ", m_topic=" + m_topic
		      + ", m_partition=" + m_partition + ", m_offset=" + m_offset + ", m_size=" + m_size + ", m_expireTime="
		      + m_expireTime + "]";
	}
}
