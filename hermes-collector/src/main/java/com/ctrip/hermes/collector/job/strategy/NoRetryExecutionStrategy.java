package com.ctrip.hermes.collector.job.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.job.Job;
import com.ctrip.hermes.collector.job.JobContext;


@Component
public class NoRetryExecutionStrategy implements ExecutionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoRetryExecutionStrategy.class);

    @Override
    public void start(Job job, JobContext context) {
        try {
            job.run(context);
        } catch (Exception e) {
            LOGGER.error("Job [{}, {}] throws unexpected exception: ", context.getName(),
                    context.getGroup(), e);
        }
    }

}
