package com.ctrip.hermes.collector.job.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.job.Job;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.service.JobStatusService;

@Component
public class DelayedRescheduleExecutionStrategy extends RetryExecutionStrategy {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(DelayedRescheduleExecutionStrategy.class);

    @Autowired
    private CollectorConfiguration m_conf;

    @Autowired
    private JobStatusService m_jobStatusService;

    @Override
    public void start(Job job, JobContext context) {
        try {
            readExecutionContext(context);
            while (!context.isSucceed() || context.hasMissedJobs()) {
            	if (context.isSucceed() && context.hasMissedJobs()) {
            		context.setLastScheduledExecutionTime(context.getScheduledExecutionTime());
                    context.setScheduledExecutionTime(context.nextScheduledExecutionTime());
            	}
                context.setSucceed(false);
                super.start(job, context);
                saveExecutionContext(context);
            }
        } catch (Exception e) {
            LOGGER.error("Job [{}, {}] fails to read/save context: {}", context.getName(),
                    context.getGroup(), e.getMessage(), e);
        }
    }
    
    private boolean readExecutionContext(JobContext context) throws Exception {
        if (m_conf.isEnableLogJobStatus() && m_conf.isEnableJobReschedule()) {
            m_jobStatusService.readExecutionContext(context);
        }
        return true;
    }
    
    private boolean saveExecutionContext(JobContext context) throws Exception {
        if (m_conf.isEnableLogJobStatus() && m_conf.isEnableJobReschedule()) {
            m_jobStatusService.saveExecutionContext(context);
        }
        return true;
    }
}
