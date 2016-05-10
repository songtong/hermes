package com.ctrip.hermes.collector.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.ctrip.hermes.collector.hub.DataHub;
import com.ctrip.hermes.collector.job.annotation.Job;

public abstract class ScheduledJob implements ApplicationContextAware {
	private static Logger LOGGER = LoggerFactory.getLogger(ScheduledJob.class);

	private ApplicationContext m_applicationContext;
	
	// Preparation phase for job.
	public abstract void setup(JobContext context);
	
	// Actually run the job.
	public abstract boolean run(JobContext context) throws Exception;
	
	public final void schedule(JobContext context) {
		if (context == null) {
			context = createDefaultContext();
		}
				
		LOGGER.info(String.format("Scheduled job kickoff: [%s, %s]", context.getName(), context.getGroup()));
		setup(context);
		try {
			if (run(context)) {
				LOGGER.info(String.format("Scheduled job success: [%s, %s]", context.getName(), context.getGroup()));
				success(context);
				context.setSucceed(true);
			} else {
				LOGGER.info(String.format("Scheduled job failed: [%s, %s]", context.getName(), context.getGroup()));
				fail(context);
			}
		} catch (Exception e) {
			context.setHasError(true);
			LOGGER.error(String.format("Scheduled job failed due to exception: [%s, %s]", context.getName(), context.getGroup()), e);
		} finally {
			complete(context);
		}
		
		LOGGER.info(String.format("Scheduled job done: [%s, %s]", context.getName(), context.getGroup()));
	}
	
	public final JobContext createDefaultContext() {
		JobContext context = new JobContext();
		if (this.getClass().isAnnotationPresent(Job.class)) {
			Job identifier = this.getClass().getAnnotation(Job.class);
			if (identifier.name().equals("")) {
				context.setName(this.getClass().getSimpleName());
			} else {
				context.setName(identifier.name());
			}
			context.setGroup(identifier.group());
		}
		return context;
	}
	
	// Method called no matter the job failed or succeed.
	public void complete(JobContext context) {
		// Only if the whole workflow is completed and there is no error encountered, then submit the record.
		if (context.commitable()) {
			m_applicationContext.getBean(DataHub.class).offer(context.getRecord());
		}
	}
	
	// Method called when the job succeeds.
	public abstract void success(JobContext context);
	
	// Method called when the job fails.
	public abstract void fail(JobContext context);

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.m_applicationContext = applicationContext;
	}
	
	public ApplicationContext getApplicationContext() {
		return m_applicationContext;
	}
}
