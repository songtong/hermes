package com.ctrip.hermes.consumer.engine.pipeline;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.Hermes;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessageAware;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.service.SystemClockService;
import com.dianping.cat.Cat;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = PipelineSink.class, value = BuildConstants.CONSUMER)
public class DefaultConsumerPipelineSink implements PipelineSink<Void> {

	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerPipelineSink.class);

	@Inject
	private SystemClockService m_systemClockService;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Void handle(PipelineContext<Void> ctx, Object payload) {
		Pair<ConsumerContext, List<ConsumerMessage<?>>> pair = (Pair<ConsumerContext, List<ConsumerMessage<?>>>) payload;

		MessageListener consumer = pair.getKey().getConsumer();
		List<ConsumerMessage<?>> msgs = pair.getValue();
		setOnMessageStartTime(msgs);
		logVersionToCat();
		try {
			consumer.onMessage(msgs);
		} catch (Throwable e) {
			log.error(
			      "Uncaught exception occurred while calling MessageListener's onMessage method, will nack all messages which handled by this call.",
			      e);
			for (ConsumerMessage<?> msg : msgs) {
				msg.nack();
			}
		} finally {
			for (ConsumerMessage<?> msg : msgs) {
				// ensure every message is acked or nacked, ack it if not
				msg.ack();
			}
		}

		return null;
	}

	private void setOnMessageStartTime(List<ConsumerMessage<?>> msgs) {
		for (ConsumerMessage<?> msg : msgs) {
			if (msg instanceof BaseConsumerMessageAware) {
				BaseConsumerMessage<?> baseMsg = ((BaseConsumerMessageAware<?>) msg).getBaseConsumerMessage();
				baseMsg.setOnMessageStartTimeMills(m_systemClockService.now());
			}
		}
	}

	private void logVersionToCat() {
		Cat.logEvent("Hermes.Client.Version", Hermes.VERSION);
	}

}
