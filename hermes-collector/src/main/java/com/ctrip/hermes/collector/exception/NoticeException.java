package com.ctrip.hermes.collector.exception;

public class NoticeException extends Exception {
	public NoticeException(String message) {
		super(message);
	}
	
	public NoticeException(Throwable t) {
		super(t);
	}
	
	public NoticeException(String message, Throwable t) {
		super(message, t);
	}
}
