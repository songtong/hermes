package com.ctrip.hermes.collector.job.strategy;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.job.Job;
import com.ctrip.hermes.collector.job.JobContext;

@Component
public class RetryUtilSuccessExecutionStrategy extends RetryExecutionStrategy {
    public void start(Job job, JobContext context) {
    	context.setRetries(Integer.MAX_VALUE);
    	super.start(job, context);
    }
}
