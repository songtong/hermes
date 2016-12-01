package com.ctrip.hermes.collector.exception;

public class RequiredException extends Exception {
	public RequiredException(String message) {
		super(message);
	}
	
	public RequiredException(Throwable t) {
		super(t);
	}
	
	public RequiredException(String message, Throwable t) {
		super(message, t);
	}
}
