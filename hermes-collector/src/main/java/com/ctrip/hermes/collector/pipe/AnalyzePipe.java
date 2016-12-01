package com.ctrip.hermes.collector.pipe;


import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public abstract class AnalyzePipe<T> extends Pipe<T> implements Analyze<T>, ApplicationContextAware {
	private ApplicationContext m_applicationContext;
	private ThreadLocal<PipeContext> m_pipeContexts = new ThreadLocal<PipeContext>();

	public AnalyzePipe(Class<?> clzz) {
		super(clzz);
	}

	@Override
	public void doProcess(PipeContext context, T record) throws Exception {
		m_pipeContexts.set(context);
		setup(record);
		try {
			analyze(record);
		} catch (Exception e) {
			throw e;
		} finally {
			complete(record);
			m_pipeContexts.set(null);
		}
	}
	
	public PipeContext getPipeContext() {
		return m_pipeContexts.get();
	}
	
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.m_applicationContext = applicationContext;
	}
	
	public ApplicationContext getApplicationContext() {
		return this.m_applicationContext;
	}
}
