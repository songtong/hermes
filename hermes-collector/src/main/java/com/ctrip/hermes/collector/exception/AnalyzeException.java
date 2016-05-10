package com.ctrip.hermes.collector.exception;

/**
 * @author tenglinxiao
 * @mail lxteng@ctrip.com
 * @since 0.0.1
 */
public class AnalyzeException extends RuntimeException {
	public AnalyzeException(String message) {
		super(message);
	}
	
	public AnalyzeException(Throwable t) {
		super(t);
	}
	
	public AnalyzeException(String message, Throwable t) {
		super(message, t);
	}
}
