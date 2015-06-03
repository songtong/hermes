package com.ctrip.hermes.portal.service.storage.exception;

public class TopicIsNullException extends Exception {
	public TopicIsNullException(String message) {
		super(message);
	}

	public TopicIsNullException(Throwable cause) {
		super(cause);
	}
}
