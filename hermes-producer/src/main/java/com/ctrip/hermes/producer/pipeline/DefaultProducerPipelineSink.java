package com.ctrip.hermes.producer.pipeline;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.Hermes;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.sender.MessageSender;
import com.dianping.cat.Cat;

public class DefaultProducerPipelineSink implements PipelineSink<Future<SendResult>> {
	@Inject
	private MessageSender messageSender;

	@Inject
	private ProducerConfig m_config;

	private AtomicLong m_lastLogVersionTime = new AtomicLong(0);

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
		logVersionToCat();
		return messageSender.send((ProducerMessage<?>) input);
	}

	private void logVersionToCat() {
		if (m_config.isCatEnabled()) {
			long now = System.currentTimeMillis();
			if (now - m_lastLogVersionTime.get() > 60000) {
				Cat.logEvent("Hermes.Client.Version", Hermes.VERSION);
				m_lastLogVersionTime.set(now);
			}
		}
	}
}
