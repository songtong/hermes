package com.ctrip.hermes.collector.job.strategy;

import com.ctrip.hermes.collector.job.Job;
import com.ctrip.hermes.collector.job.JobContext;

/**
 * @author tenglinxiao
 *
 */
public interface ExecutionStrategy {
	public void start(Job job, JobContext context);
}
