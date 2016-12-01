package com.ctrip.hermes.collector.pipe;

import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordContent;

/**
 * @author tenglinxiao
 *
 */
public interface Analyze<T> {

	// Preparation phase for analysis.
	public void setup(T obj);
	
	// Do analysis.
	public void analyze(T obj) throws Exception;
	
	// Method called when analysis is done no matter the process failed or succeed.
	public void complete(T obj);

}
