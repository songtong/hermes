package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.transport.ManualRelease;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@ManualRelease
public class PullMessageAckCommand extends AbstractCommand implements Ack {
	private static final long serialVersionUID = 7125716603747372895L;

	private List<TppConsumerMessageBatch> m_batches = new ArrayList<>();

	public PullMessageAckCommand() {
		super(CommandType.ACK_MESSAGE_PULL);
	}

	public List<TppConsumerMessageBatch> getBatches() {
		return m_batches;
	}

	public void addBatches(List<TppConsumerMessageBatch> batches) {
		if (batches != null) {
			m_batches.addAll(batches);
		}
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		List<TppConsumerMessageBatch> batches = new ArrayList<>();

		readBatchMetas(codec, batches);

		readBatchDatas(buf, codec, batches);

		m_batches = batches;
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		writeBatchMetas(codec, m_batches);
		writeBatchDatas(buf, codec, m_batches);
	}

	private void writeBatchDatas(ByteBuf buf, HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		for (TppConsumerMessageBatch batch : batches) {
			// placeholder for len
			int start = buf.writerIndex();
			codec.writeInt(-1);
			int indexBeforeData = buf.writerIndex();
			batch.getTransferCallback().transfer(buf);
			int indexAfterData = buf.writerIndex();

			buf.writerIndex(start);
			codec.writeInt(indexAfterData - indexBeforeData);
			buf.writerIndex(indexAfterData);

		}
	}

	private void readBatchDatas(ByteBuf buf, HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		for (TppConsumerMessageBatch batch : batches) {
			int len = codec.readInt();
			batch.setData(buf.readSlice(len));
		}

	}

	private void readBatchMetas(HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		int batchSize = codec.readInt();
		for (int i = 0; i < batchSize; i++) {
			TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
			int seqSize = codec.readInt();
			batch.setTopic(codec.readString());
			batch.setPartition(codec.readInt());
			batch.setPriority(codec.readInt() == 0 ? true : false);
			batch.setResend(codec.readBoolean());

			for (int j = 0; j < seqSize; j++) {
				long msgSeq = codec.readLong();
				int remainingRetries = codec.readInt();
				batch.addMsgSeq(msgSeq, remainingRetries);
			}
			batches.add(batch);
		}
	}

	private void writeBatchMetas(HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		codec.writeInt(batches.size());
		for (TppConsumerMessageBatch batch : batches) {
			codec.writeInt(batch.size());
			codec.writeString(batch.getTopic());
			codec.writeInt(batch.getPartition());
			codec.writeInt(batch.isPriority() ? 0 : 1);
			codec.writeBoolean(batch.isResend());
			for (Pair<Long, Integer> pair : batch.getMsgSeqs()) {
				codec.writeLong(pair.getKey());
				codec.writeInt(pair.getValue());
			}
		}
	}
}
