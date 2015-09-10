package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseConsumingStrategy implements ConsumingStrategy {

	@Inject
	protected ConsumerConfig m_config;

	@Override
	public SubscribeHandle start(ConsumerContext context, int partitionId) {

		try {
			int localCacheSize = m_config.getLocalCacheSize(context.getTopic().getName());

			final ConsumerTask consumerTask = getConsumerTask(context, partitionId, localCacheSize);

			Thread thread = HermesThreadFactory
			      .create(
			            String.format("ConsumerThread-%s-%s-%s", context.getTopic().getName(), partitionId,
			                  context.getGroupId()), false).newThread(new Runnable() {

				      @Override
				      public void run() {
					      consumerTask.start();
				      }
			      });

			thread.start();

			SubscribeHandle subscribeHandle = new SubscribeHandle() {

				@Override
				public void close() {
					consumerTask.close();
				}
			};

			return subscribeHandle;
		} catch (Exception e) {
			throw new RuntimeException(String.format("Failed to start consumer(topic=%s, partition=%s, groupId=%s).",
			      context.getTopic().getName(), partitionId, context.getGroupId()), e);
		}
	}

	protected abstract ConsumerTask getConsumerTask(ConsumerContext context, int partitionId, int localCacheSize);

}
