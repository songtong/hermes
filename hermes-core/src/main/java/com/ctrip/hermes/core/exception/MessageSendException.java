package com.ctrip.hermes.core.exception;

public class MessageSendException extends Exception {

	private static final long serialVersionUID = 1L;

	public MessageSendException() {
		super();
	}

	public MessageSendException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MessageSendException(String message, Throwable cause) {
		super(message, cause);
	}

	public MessageSendException(String message) {
		super(message);
	}

	public MessageSendException(Throwable cause) {
		super(cause);
	}

}
