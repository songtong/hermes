package com.ctrip.hermes.collector.pipe;

/**
 * @author tenglinxiao
 *
 **/
public interface Processor<T> {
	public void process(ProcessContext ctx, T obj);
	
	public interface ProcessContext {};
}
