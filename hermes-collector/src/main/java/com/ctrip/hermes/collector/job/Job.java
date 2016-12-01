package com.ctrip.hermes.collector.job;

/**
 * @author tenglinxiao
 *
 */
public interface Job {
	
	// Run the job.
	public boolean run(JobContext ctx) throws Exception;
	
	public enum Status {
		SUCCESS, FAILED, RUNNING, INITIAL
	}
}
 