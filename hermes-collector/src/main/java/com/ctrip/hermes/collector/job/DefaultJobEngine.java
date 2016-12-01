package com.ctrip.hermes.collector.job;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.elasticsearch.common.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.SimpleTriggerContext;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;
import com.ctrip.hermes.collector.utils.TimeUtils;

@Component
public class DefaultJobEngine implements JobEngine, ApplicationContextAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJobEngine.class);
    private ApplicationContext m_applicationContext;

    @Autowired
    private JobExecutor m_jobExecutor;

    @Autowired
    private CollectorConfiguration m_conf;

    private ThreadPoolTaskScheduler m_scheduler;

    private Map<Job, Pair<Trigger, Date>> m_triggers = new HashMap<>();

    @PostConstruct
    private void init() {
        m_scheduler = new ThreadPoolTaskScheduler();
        m_scheduler.setPoolSize(m_conf.getSchedulerPoolSize());
        m_scheduler.setThreadFactory(CollectorThreadFactory.newFactory(DefaultJobEngine.class));
        m_scheduler.initialize();
        bootstrap();
    }

    @Override
    public void bootstrap() {
        Map<String, Job> jobs = m_applicationContext.getBeansOfType(Job.class);
        for (final Job job : jobs.values()) {
            JobDescription description = job.getClass().getAnnotation(JobDescription.class);
            if (description == null || StringUtils.isEmpty(description.cron())) {
                LOGGER.warn("Failed to arrange scheduling for job: {}", job.getClass().getName());
                continue;
            }

            Trigger trigger = new CronTrigger(description.cron());
            m_triggers.put(job, new Pair<Trigger, Date>(trigger, trigger.nextExecutionTime(createDefaultTriggerContext())));

            m_scheduler.schedule(new Runnable() {

                @Override
                public void run() {
                    Pair<Trigger, Date> trigger = m_triggers.get(job);
                    
                    // Scheduled date computed before schedule.
                    Date scheduledDate = trigger.getValue();

                    SimpleTriggerContext newTriggerContext =
                            new SimpleTriggerContext(scheduledDate, scheduledDate, scheduledDate);

                    // Computed scheduled date from now.
                    Date nextExecutionTime =
                            trigger.getKey().nextExecutionTime(createDefaultTriggerContext());

                    if (trigger.getKey().nextExecutionTime(newTriggerContext).getTime() == nextExecutionTime
                            .getTime()) {
                        m_jobExecutor.execute(job, scheduledDate);
                        LOGGER.info("Succeed to kickoff scheduling job on time [{}]: {}",
                                TimeUtils.formatDate(scheduledDate), job.getClass().getName());
                    } else {
                        LOGGER.error(
                                "Missed to kickoff scheduling job due to time unmatched [{}, {}]: {}",
                                TimeUtils.formatDate(scheduledDate),
                                TimeUtils.formatDate(nextExecutionTime), job.getClass().getName());
                    }
                    trigger.setValue(trigger.getKey().nextExecutionTime(newTriggerContext));
                    m_triggers.put(job, trigger);
                }
            }, trigger);
            LOGGER.info("Succeed to add job into scheduler: {}", job.getClass().getName());
        }
    }

    private TriggerContext createDefaultTriggerContext() {
        Date currentTimestamp = Calendar.getInstance().getTime();
        return new SimpleTriggerContext(currentTimestamp, currentTimestamp, currentTimestamp);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.m_applicationContext = applicationContext;
    }

}
