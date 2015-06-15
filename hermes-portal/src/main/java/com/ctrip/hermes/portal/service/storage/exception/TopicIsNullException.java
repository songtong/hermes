package com.ctrip.hermes.portal.service.storage.exception;

public class TopicIsNullException extends Exception {
	/**
	 * 
	 */
   private static final long serialVersionUID = -9085158242447065551L;

	public TopicIsNullException(String message) {
		super(message);
	}

	public TopicIsNullException(Throwable cause) {
		super(cause);
	}
}
