package com.ctrip.hermes.consumer.engine.notifier;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerNotifier.class)
public class DefaultConsumerNotifier implements ConsumerNotifier {

	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerNotifier.class);

	private ConcurrentMap<Long, Pair<ConsumerContext, ExecutorService>> m_consumerContexs = new ConcurrentHashMap<>();

	@Inject(BuildConstants.CONSUMER)
	private Pipeline<Void> m_pipeline;

	@Inject
	private ConsumerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private ClientEnvironment m_clientEnv;

	@Override
	public void register(long correlationId, final ConsumerContext context) {
		try {
			if (log.isDebugEnabled()) {
				log.debug("Registered(correlationId={}, topic={}, groupId={}, sessionId={})", correlationId, context
				      .getTopic().getName(), context.getGroupId(), context.getSessionId());
			}

			int threadCount = Integer.valueOf(m_clientEnv.getConsumerConfig(context.getTopic().getName()).getProperty(
			      "consumer.notifier.threadcount", m_config.getDefaultNotifierThreadCount()));

			m_consumerContexs.putIfAbsent(
			      correlationId,
			      new Pair<>(context, Executors.newFixedThreadPool(
			            threadCount,
			            HermesThreadFactory.create(
			                  String.format("ConsumerNotifier-%s-%s-%s", context.getTopic().getName(),
			                        context.getGroupId(), correlationId), false))));
		} catch (Exception e) {
			throw new RuntimeException("Register consumer notifier failed", e);
		}
	}

	@Override
	public void deregister(long correlationId) {

		Pair<ConsumerContext, ExecutorService> pair = m_consumerContexs.remove(correlationId);
		ConsumerContext context = pair.getKey();
		if (log.isDebugEnabled()) {
			log.debug("Deregistered(correlationId={}, topic={}, groupId={}, sessionId={})", correlationId, context
			      .getTopic().getName(), context.getGroupId(), context.getSessionId());
		}
		pair.getValue().shutdown();
		return;
	}

	@Override
	public void messageReceived(final long correlationId, final List<ConsumerMessage<?>> msgs) {
		Pair<ConsumerContext, ExecutorService> pair = m_consumerContexs.get(correlationId);
		final ConsumerContext context = pair.getKey();
		ExecutorService executorService = pair.getValue();

		executorService.submit(new Runnable() {

			@SuppressWarnings("rawtypes")
			@Override
			public void run() {
				try {
					for (ConsumerMessage<?> msg : msgs) {
						if (msg instanceof BrokerConsumerMessage) {
							BrokerConsumerMessage bmsg = (BrokerConsumerMessage) msg;
							bmsg.setCorrelationId(correlationId);
							bmsg.setGroupId(context.getGroupId());
						}
					}

					m_pipeline.put(new Pair<ConsumerContext, List<ConsumerMessage<?>>>(context, msgs));
				} catch (Exception e) {
					log.error(

					"Exception occurred while calling messageReceived(correlationId={}, topic={}, groupId={}, sessionId={})",
					      correlationId, context.getTopic().getName(), context.getGroupId(), context.getSessionId(), e);
				}
			}
		});

	}

	@Override
	public ConsumerContext find(long correlationId) {
		Pair<ConsumerContext, ExecutorService> pair = m_consumerContexs.get(correlationId);
		return pair == null ? null : pair.getKey();
	}

}
