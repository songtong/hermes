package com.ctrip.hermes.collector.pipe;



public abstract class ConversionPipe<T, S> extends Pipe<T> {
	private Pipe<S> m_next;
	private boolean m_continueNext;
	
	public ConversionPipe(Class<?> clazz) {
		super(clazz);
	}
	
	public void doProcess(PipeContext context, T obj) throws Exception{
		S s = doProcess0(context, obj);
		if (m_continueNext) {
			resumeProcess(context, s);
		}
	}
	
	public abstract S doProcess0(PipeContext context, T obj) throws Exception;
	
	public void resumeProcess(PipeContext context, S obj) {
		// Verify whether should abort the pipe handling.
		if ((Feature.ABORT_ON_CONTEXT.enabled(DEFAULT_FEATURES) || Feature.ABORT_ON_CONTEXT.enabled(m_features)) && context.isAbort()) {
			return;
		}
				
		if (m_next != null) {
			m_next.process(context, obj);
		}
	}
	
	public void setConversionNext(Pipe<S> pipe) {
		this.m_next = pipe;
	}
	
	public boolean isContinueNext() {
		return m_continueNext;
	}
	
	public void setContinueNext(boolean continueNext) {
		this.m_continueNext = continueNext;
	}
	
	public void setNext(Pipe<T> pipe) {
		throw new RuntimeException("Can NOT set next for this pipe, use [setConversionNext] instead!");
	}
}
