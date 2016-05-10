package com.ctrip.hermes.collector.pipe;


public abstract class ConversionPipe<T, S> extends Pipe<T> {
	public ConversionPipe(Class<?> clzz) {
		super(clzz);
		// TODO Auto-generated constructor stub
	}

	private Pipe<S> m_next;
	
	public void resumeProcess(PipeContext context, S obj) {
		if (m_next != null) {
			m_next.process(context, obj);
		}
	}
	
	public void setHubNext(Pipe<S> pipe) {
		this.m_next = pipe;
	}
	
	public void setNext(Pipe<T> pipe) {
		throw new RuntimeException("Can NOT set next for this pipe, but only glue next!");
	}

	@Override
	public boolean hasNext() {
		return this.m_next != null;
	}
}
