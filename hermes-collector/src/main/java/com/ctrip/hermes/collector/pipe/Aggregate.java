package com.ctrip.hermes.collector.pipe;


/**
 * @author tenglinxiao
 * 
 */
public interface Aggregate<T> {

	// Preparation phase for aggregation.
	public void setup(T obj);
	
	// Do aggregation.
	public abstract boolean aggregate(T obj);
	
	// Method called when aggregation is done no matter the process failed or succeed.
	public void complete(T obj);
	
}
