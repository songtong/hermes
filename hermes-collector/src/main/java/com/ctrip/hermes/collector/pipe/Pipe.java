package com.ctrip.hermes.collector.pipe;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tenglinxiao
 * @mail lxteng@ctrip.com
 */
public abstract class Pipe<T> {
	// Default settings for features.
	private static final int DEFAULT_FEATURES = Feature.and(Feature.ABORT_ON_CONTEXT, Feature.ABORT_ON_ERROR);
	private static final Logger LOGGER = LoggerFactory.getLogger(Pipe.class);
	private Pipe<T> m_next;
	private int m_features;
	private Class<?> m_clzz;
	
	public Pipe(Class<?> clzz) {
		this.m_clzz = clzz;
	}
	
	public Class<?> getTypeClass() {
		return m_clzz;
	}
	
	// Test whether it's valid for this pipe.
	public abstract boolean validate(T obj);
	
	public final void process(PipeContext context, T obj) {
		// If fail the validation, abort the process.
		if (!validate(obj)) {
			context.abort();
		}
		
		// Verify whether should abort the pipe handling.
		if ((Feature.ABORT_ON_CONTEXT.enabled(DEFAULT_FEATURES) || Feature.ABORT_ON_CONTEXT.enabled(m_features)) && context.isAbort()) {
			return;
		}
		
		try {
			doProcess(context, obj);
		} catch (Exception e) {
			LOGGER.error("Pipe encountered an error: " + e.getMessage(), e);
			if (Feature.ABORT_ON_ERROR.enabled(DEFAULT_FEATURES) || Feature.ABORT_ON_ERROR.enabled(m_features)) {
				context.abort();
				return;
			}
		}
		
		// Handle next pipe.
		processNext(context, obj);
	}
	
	// Method for complete pipe processing.
	public abstract void doProcess(PipeContext context, T obj) throws Exception;
	
	public void processNext(PipeContext context, T obj) {
		// Verify whether should abort the pipe handling.
		if ((Feature.ABORT_ON_CONTEXT.enabled(DEFAULT_FEATURES) || Feature.ABORT_ON_CONTEXT.enabled(m_features)) && context.isAbort()) {
			return;
		}
		
		// Process next pine if it has.
		if (m_next != null) {
			this.m_next.process(context, obj); 
		}
	}
	
	// Set next pipeline.
	public void setNext(Pipe<T> next) {
		this.m_next = next;
	}
	
	// Change default features.
	public void setFeatures(int features) {
		this.m_features = features;
	}
	
	// Test whether it has next pipeline.
	public boolean hasNext() {
		return m_next != null;
	}
	
	public enum Feature {
		CONTINUE_ALWAYS(0x1),
		ABORT_ON_CONTEXT(0x2),
		CONTINUE_ON_ERROR(0x4),
		ABORT_ON_ERROR(0x8);
		
		private int m_code;
		private Feature(int code) {
			this.m_code = code;
		} 
		
		private int getCode() {
			return m_code;
		}
		
		private boolean enabled(int features) {
			return (this.m_code & features) > 0;
		}
		
		public static int and(Feature... features) {
			int code = 0;
			for (Feature f : features) {
				code |= f.getCode();
			}
			return code;
		}
	}
}
