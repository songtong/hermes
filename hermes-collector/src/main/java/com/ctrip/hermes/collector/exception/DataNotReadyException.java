package com.ctrip.hermes.collector.exception;

@SuppressWarnings("serial")
public class DataNotReadyException extends Exception {
	public DataNotReadyException(String message) {
		super(message);
	}
	
	public DataNotReadyException(Throwable t) {
		super(t);
	}
	
	public DataNotReadyException(String message, Throwable t) {
		super(message, t);
	}
}
