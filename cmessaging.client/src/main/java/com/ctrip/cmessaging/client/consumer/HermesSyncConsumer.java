package com.ctrip.cmessaging.client.consumer;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.ISyncConsumer;
import com.ctrip.cmessaging.client.exception.ConsumeTimeoutException;
import com.ctrip.cmessaging.client.message.HermesIMessage;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class HermesSyncConsumer implements ISyncConsumer {

	private long timeout;

	BlockingDeque<ConsumerMessage<byte[]>> msgQueue = new LinkedBlockingDeque<>();

	public HermesSyncConsumer(String topic, String groupId, long timeout) {
		this.timeout = timeout;

		Engine engine = PlexusComponentLocator.lookup(Engine.class);

		// todo: move engine.start() into consumeOne() step,
		// 暂时无法实现，需engine提供stop方法。
		engine.start(new Subscriber(topic, groupId, new InnerConsumer()));
	}

	@Override
	public IMessage consumeOne() throws ConsumeTimeoutException {
		try {
			ConsumerMessage<byte[]> consumerMessage = msgQueue.poll(timeout, TimeUnit.MILLISECONDS);

			if (null == consumerMessage) {
				throw new ConsumeTimeoutException();
			} else {
				return new HermesIMessage(consumerMessage, true);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setReceiveTimeout(long l) {
		this.timeout = l;
	}

	public void topicBind(String topic, String exchangeName) {
	}

	@Override
	public void setBatchSize(int i) {
		/*
		 * do nothing
		 */
	}

	@Override
	public void setIdentifier(String identifier) {
	}

	class InnerConsumer implements MessageListener<byte[]> {
		@Override
		public void onMessage(List<ConsumerMessage<byte[]>> msgs) {
			msgQueue.addAll(msgs);
		}
	}
}
