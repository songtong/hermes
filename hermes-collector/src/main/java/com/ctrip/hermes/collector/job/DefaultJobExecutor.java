package com.ctrip.hermes.collector.job;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.job.annotation.JobStrategy;
import com.ctrip.hermes.collector.job.strategy.ExecutionStrategy;
import com.ctrip.hermes.collector.job.strategy.NoRetryExecutionStrategy;
import com.ctrip.hermes.collector.service.JobStatusService;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;

@Component
public class DefaultJobExecutor implements JobExecutor, ApplicationContextAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJobExecutor.class);
    private ExecutorService m_executor;
    private ApplicationContext m_applicationContext;
    private ConcurrentHashMap<Pair<String, JobGroup>, AtomicBoolean> m_runnings =
            new ConcurrentHashMap<>();

    @Autowired
    private CollectorConfiguration m_conf;

    @Autowired
    private JobStatusService m_jobStatusService;

    @PostConstruct
    private void init() {
        m_executor =
                Executors.newFixedThreadPool(m_conf.getExecutorPoolSize(),
                        CollectorThreadFactory.newFactory(DefaultJobExecutor.class));
    }

    @Override
    public void execute(final Job job, final Date scheduledDate) {
        m_executor.submit(new Runnable() {
            public void run() {
            	try {
	                JobContext context = createJobContext(job);
	                context.setScheduledExecutionTime(scheduledDate);
	                Pair<String, JobGroup> jobKey = new Pair<>(context.getName(), context.getGroup());
	                m_runnings.putIfAbsent(jobKey, new AtomicBoolean(false));
	                AtomicBoolean running = m_runnings.get(jobKey);
	                if (running.compareAndSet(false, true)) {
	                    try {
	                        context.getExecutionStrategy().start(job, context);
	                    } finally {
	                        running.compareAndSet(true, false);
	                    }
	                }
            	} catch (Exception e) {
            		LOGGER.error("Failed to run job [{}] due to exception: {}", job.getClass().getName(), e.getMessage(), e);
            	}
            }
        });
    }

    private JobContext createJobContext(Job job) {
        JobContext context = createDefaultJobContext(job);
        JobStrategy strategy = job.getClass().getAnnotation(JobStrategy.class);
        Class<? extends ExecutionStrategy> strategyClazz = NoRetryExecutionStrategy.class;
        if (strategy != null) {
            strategyClazz = strategy.value();
            context.setRetries(strategy.retries());
            context.setRetryDelay(strategy.retryDelay());
        }

        context.setExecutionStrategy((ExecutionStrategy)findExactMatch(strategyClazz));
        return context;
    }

    private JobContext createDefaultJobContext(Job job) {
        JobContext context = new JobContext();
        // Default
        context.setName(job.getClass().getSimpleName());
        context.setGroup(JobGroup.DEFAULT);

        if (job.getClass().isAnnotationPresent(JobDescription.class)) {
            JobDescription identifier = job.getClass().getAnnotation(JobDescription.class);
            if (!identifier.name().equals("")) {
                context.setName(identifier.name());
            }
            context.setGroup(identifier.group());
            context.setTrigger(new CronTrigger(identifier.cron()));
        }
        
        return context;
    }
    
    private <T> T findExactMatch(Class<?> clazz) {
    	Map<String, ?> beans = m_applicationContext.getBeansOfType(clazz);
    	for (Object bean : beans.values()) {
    		if (bean.getClass() == clazz) {
    			return (T)bean;
    		}
    	}
    	return null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.m_applicationContext = applicationContext;
    }
}
