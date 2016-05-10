package com.ctrip.hermes.collector.pipe;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordContent;

@Component
public class PipelineManager implements ApplicationContextAware {
	private ApplicationContext context;
	private Map<String, Pipe> pipelines = new HashMap<String, Pipe>();
	
	@PostConstruct
	public void init() {
		//System.out.println(context.getBean(Pipeline.class));
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.context = applicationContext;
	}
	
	public Pipe findCommandPipeline(Record<? extends RecordContent> command) {
		return null;
	}
}
