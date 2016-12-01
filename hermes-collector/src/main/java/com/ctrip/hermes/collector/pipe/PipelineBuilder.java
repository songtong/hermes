package com.ctrip.hermes.collector.pipe;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.collector.exception.RequiredException;

public class PipelineBuilder {
	private static Logger logger = LoggerFactory.getLogger(PipelineBuilder.class);
	private Pipes m_pipelines = new Pipes(null);
	private Pipe m_tailPipeline = null;
	private Pipe m_newPipeline = null;
	
	private void newPipeline(Class<? extends Pipe> clz) {
		Constructor<? extends Pipe> constructor;
		try {
			constructor = clz.getConstructor();
			if (!constructor.isAccessible()) {
				constructor.setAccessible(true);
			}
			m_newPipeline = constructor.newInstance();
		} catch (Exception e) {
			logger.error(String.format("Default constructor MUST be defined for pipeline class: %s", clz.getName()), e);
		}
	}
	
	public PipelineBuilder pipeline(Class<? extends Pipe> clz) {
		newPipeline(clz);
		m_tailPipeline = m_newPipeline;
		m_pipelines.addPipe(m_newPipeline);
		return this;
	}
	
	public PipelineBuilder connect(Class<? extends Pipe> clz) throws RequiredException {
		newPipeline(clz);
		m_tailPipeline.setNext(m_newPipeline);
		m_tailPipeline = m_newPipeline;
		return this;
	}
	
	public Pipe build() {
		return m_pipelines;
	}
}
