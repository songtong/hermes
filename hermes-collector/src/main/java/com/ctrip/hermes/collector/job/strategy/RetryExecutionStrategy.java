package com.ctrip.hermes.collector.job.strategy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.job.Job;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.utils.TimeUtils;

@Component
public class RetryExecutionStrategy implements ExecutionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryExecutionStrategy.class);

    @Override
    public void start(Job job, JobContext context) {
        LOGGER.info("Job [{}, {}] triggered at [{}] with strategy {}.", context.getName(), context
                .getGroup(), TimeUtils.formatDate(context.getScheduledExecutionTime()), this
                .getClass().getName());
        while (!context.isSucceed()) {
            try {
                job.run(context);
            } catch (Exception e) {
                LOGGER.error("Job [{}, {}] throws unexpected exception: ", context.getName(),
                        context.getGroup(), e);
            }

            if (context.isSucceed() || !context.retry()) {
                break;
            }

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(context.getRetryDelay()));
            LOGGER.info("Job [{], {}] retried: {} times left", context.getName(),
                    context.getGroup(), context.getRetries());

        }
    }

}
