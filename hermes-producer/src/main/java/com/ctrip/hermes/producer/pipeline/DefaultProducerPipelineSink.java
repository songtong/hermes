package com.ctrip.hermes.producer.pipeline;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.Hermes;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.sender.MessageSender;
import com.dianping.cat.Cat;

public class DefaultProducerPipelineSink implements PipelineSink<Future<SendResult>> {
	@Inject
	private MessageSender messageSender;

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
		logVersionToCat();
		return messageSender.send((ProducerMessage<?>) input);
	}

	private void logVersionToCat() {
		Cat.logEvent("Hermes.Client.Version", Hermes.VERSION);
	}
}
