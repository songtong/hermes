package com.ctrip.hermes.collector.pipe;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.collector.hub.DataHub;
import com.ctrip.hermes.collector.hub.StateHub;
import com.ctrip.hermes.collector.pipeline.annotation.Pipeline;
import com.ctrip.hermes.collector.pipeline.annotation.Pipelines;
import com.ctrip.hermes.collector.rule.RulesEngine;

//@Component
public class PipelineRegistry implements ApplicationContextAware {
	private ApplicationContext m_applicationContext;
	private Map<Class<?>, Map<String, Pipes<?>>> m_pipelines = new HashMap<Class<?>, Map<String, Pipes<?>>>();
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@PostConstruct
	protected void init() {
		Map<String, Pipe> pipeBeans = m_applicationContext.getBeansOfType(Pipe.class);
		
		// Pipeline handled type & declaration mapping. 
		Map<Class<?>, List<Triple<String, Integer, String>>> typedPipes = new HashMap<Class<?>, List<Triple<String, Integer, String>>>();
		List<Triple<String, Integer, String>> pipes = null;
		String pipeName = null;
		for (Map.Entry<String, Pipe> entry : pipeBeans.entrySet()) {
			Pipe pipe = entry.getValue();
//			if (pipe.getClass() == DataHub.class || pipe.getClass() == StateHub.class || pipe.getClass() == RulesEngine.class) {
//				continue;
//			}
						
			if (!typedPipes.containsKey(pipe.getTypeClass())) {
				typedPipes.put(pipe.getTypeClass(), new ArrayList<Triple<String, Integer, String>>());
			}
			
			pipes = typedPipes.get(pipe.getTypeClass());
			
			// Handle pipeline annotation.
			Pipeline pipeline = pipe.getClass().getAnnotation(Pipeline.class);
			if (pipeline != null) {
				pipeName = pipeline.name().equals("")? pipe.getClass().getSimpleName(): pipeline.name();
				pipes.add(new Triple<String, Integer, String>(pipeName, pipeline.order(), entry.getKey()));
			}
			
			// Handle pipelines annotation.
			Pipelines pipelines = pipe.getClass().getAnnotation(Pipelines.class);
			if (pipelines != null) {
				for (Pipeline p : pipelines.value()) {
					pipeName = p.name().equals("")? pipe.getClass().getSimpleName(): p.name();
					pipes.add(new Triple<String, Integer, String>(pipeName, p.order(), entry.getKey()));
				}
			}
			
			// Handle pipe without pipeline/pipelines annotation.
			if (pipeline == null && pipelines == null) {
				pipes.add(new Triple<String, Integer, String>(null, 0, entry.getKey()));
			}
		}
		
		// Pipelines map.
		for (Map.Entry<Class<?>, List<Triple<String, Integer, String>>> entry : typedPipes.entrySet()) {
			// Sort the pipes by group, order.
			Collections.sort(entry.getValue(), new Comparator<Triple<String, Integer, String>>() {
				@Override
				public int compare(Triple<String, Integer, String> pipe1, Triple<String, Integer, String> pipe2) {
					if (pipe1.getFirst() == null) {
						return Integer.MIN_VALUE;
					} 
					
					if (pipe2.getFirst() == null) {
						return Integer.MAX_VALUE;
					}
					
					int compare = 0;
					if ((compare = pipe1.getFirst().compareTo(pipe2.getFirst())) == 0) {
						return pipe1.getMiddle() - pipe2.getMiddle();
					} 
					return compare;
				}
			});
		
			Pipes pipeline = null;
			Triple<String, Integer, String> lastPipe = null;
			Map<String, Pipes<?>> pipelines = new HashMap<String, Pipes<?>>();

			// Load pipes into separate pipeline.
			for (Triple<String, Integer, String> pipe : entry.getValue()) {				
				Pipe<?> pipeBean = pipeBeans.get(pipe.getLast());
				if (pipeline == null) {
					pipeline = new Pipes(pipeBean.getTypeClass());
					pipeline = new Pipes(pipe.getClass());
					pipeline.addPipe(pipeBean);
				} else {
					if (pipe.getFirst() == null || (lastPipe.getFirst() != null && lastPipe.getFirst().equals(pipe.getFirst()))) {
						pipeline.addPipe(pipeBean);
					} else {
						//pipeline.addPipe(m_applicationContext.getBean(StateHub.class));
						pipelines.put(lastPipe.getFirst() == null? "default": lastPipe.getFirst(), pipeline);
						pipeline = new Pipes(pipe.getClass());
						pipeline.addPipe(pipeBean);
					}
				}
				lastPipe = pipe;
			}

			//pipeline.addPipe(m_applicationContext.getBean(StateHub.class));
			pipelines.put(lastPipe.getFirst() == null? "default": lastPipe.getFirst(), pipeline);
			m_pipelines.put(entry.getKey(), pipelines);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void registerPipe(Pipe<?> pipe) {
		Map<String, Pipes<?>> pipelines = m_pipelines.get(pipe.getTypeClass());
		if (pipelines == null) {
			pipelines = new HashMap<String, Pipes<?>>();
			m_pipelines.put(pipe.getTypeClass(), pipelines);
		}
		Pipes pipes = new Pipes(pipe.getTypeClass());
		pipes.addPipe(pipe);
		pipelines.put(pipe.getClass().getSimpleName(), pipes);
	}
	
	public Map<Type, Map<String, Pipes<?>>> findAll() {
		//return m_pipelines;
		return null;
	}
	
	public Map<String, Pipes<?>> findPipesByType(Class<?> type) {
		return m_pipelines.get(type);
	}
	
	public Pipes<?> findPipesByNameType(String pipelineName, Class<?> type) {
		Map<String, Pipes<?>> typePipes = m_pipelines.get(type);
		if (typePipes != null) {
			return typePipes.get(pipelineName);
		}
		return null;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.m_applicationContext = applicationContext;
	}
}
