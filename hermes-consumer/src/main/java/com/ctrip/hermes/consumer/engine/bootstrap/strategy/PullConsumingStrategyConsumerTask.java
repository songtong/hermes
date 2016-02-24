package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.transport.command.v3.AckMessageCommandV3;

public class PullConsumingStrategyConsumerTask extends BaseConsumerTask {

	public PullConsumingStrategyConsumerTask(ConsumerContext context, int partitionId, int localCacheSize,
	      int maxAckHolderSize) {
		super(context, partitionId, localCacheSize, DummyAckManager.INSTANCE);
	}

	private static class DummyAckManager implements AckManager {

		public static final AckManager INSTANCE = new DummyAckManager();

		private DummyAckManager() {
		}

		@Override
		public void register(long token, Tpg tpg, int maxAckHolderSize) {
		}

		@Override
		public void ack(long token, ConsumerMessage<?> msg) {
		}

		@Override
		public void nack(long token, ConsumerMessage<?> msg) {
		}

		@Override
		public void delivered(long token, ConsumerMessage<?> msg) {
		}

		@Override
		public void deregister(long token) {
		}

		@Override
		public boolean writeAckToBroker(AckMessageCommandV3 cmd) {
			return true;
		}

	}
}