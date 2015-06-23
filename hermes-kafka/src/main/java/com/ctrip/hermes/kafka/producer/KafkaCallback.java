package com.ctrip.hermes.kafka.producer;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;

public class KafkaCallback implements org.apache.kafka.clients.producer.Callback {

	private CompletionCallback<SendResult> m_callback;

	public KafkaCallback(CompletionCallback<SendResult> callback) {
		this.m_callback = callback;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (m_callback != null) {
			if (exception != null) {
				m_callback.onFailure(exception);
			} else {
				m_callback.onSuccess(new SendResult());
			}
		}
	}
}
