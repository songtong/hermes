package com.ctrip.hermes.collector.job;

import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronTrigger;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.collector.dal.job.JobStatus;
import com.ctrip.hermes.collector.dal.job.JobStatusEntity;

/**
 * @author tenglinxiao
 *
 */
public abstract class CaseJob extends AbstractJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(CaseJob.class);
	
	public void setup(JobContext context) {
		if (m_conf.isEnableLogJobStatus() && m_conf.isEnableJobReschedule()) {
//			try {
//				JobStatus status = findOrCreateJobStatus(context);
//				status.setScheduleTime(context.getScheduledExecutionTime());
//				m_jobStatusDao.updateByPK(status, JobStatusEntity.UPDATESET_FULL);
//			} catch (DalException e) {
//				LOGGER.error("Failed to update job status for schedule time: ", e);
//			}
		} else {
			// Right time on seconds/milliseconds level.
			Calendar calendar = Calendar.getInstance();
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MILLISECOND, 0);
			
			context.setScheduledExecutionTime(calendar.getTime());
		}
	}
	
	public void execute() {
		// Cause job reschedule mechanism is built upon job status logging, so use reschedule mode only
		// when both of two switches are opened.
		if (m_conf.isEnableLogJobStatus() && m_conf.isEnableJobReschedule()) {
			JobContext context = setupRescheduleContext();
			if (context.hasMissedJobs()) {
				Date executionTime = context.getScheduledExecutionTime();
				while (executionTime.getTime() <= System.currentTimeMillis()) {
					//super.execute(context);
					
					// Reschedule the job missed or failed previously until there is one failure.
					if (!context.isSucceed()) {
						break;
					}
					
					context.setLastScheduledExecutionTime(executionTime);
					executionTime = context.nextScheduledExecutionTime();
					
					// Break the loop if next execution time is equal or less than last one.
					if (context.getLastScheduledExecutionTime().getTime() >= executionTime.getTime()) {
						break;
					}
				}
			} else {
				//super.execute();
			}
		} else {
			//super.execute();
		}
	}
	
	public JobContext setupRescheduleContext () {
		// Create default job context.
		JobContext context = createDefaultContext();
		//context.setRescheduleMode(true);
		
		// Load the previous context.
		//context = readContext(context);
		return context;
	}
	
	protected JobContext createDefaultContext() {
		//JobContext context = super.createDefaultContext();
		Scheduled scheduled = findScheduledAnnotation();
		if (scheduled != null) {
			if (scheduled.cron() != null) {
			//	context.setTrigger(new CronTrigger(scheduled.cron()));
			} else {
				//TODO add trigger for other types of schedule policy.
			}
		}
		
		return null;
		//return context;
	}
		
	public boolean doRun(JobContext context) throws Exception{
		boolean isSuccess = doRun(context);
		if (isSuccess) {
			success(context);
		} else {
			fail(context);
		}
		return isSuccess;
	}
		
	// Method called when the job succeeds.
	public abstract void success(JobContext context) throws Exception;
	
	// Method called when the job fails.
	public void fail(JobContext context) throws Exception {
		// Almost has nothing to do.
	}
	
	public void complete(JobContext context) {
		// Save context when job reschedule is enabled.
		if (m_conf.isEnableJobReschedule()) {
			//saveContext(context);
		}
		super.complete(context);
	}
	
	private Scheduled findScheduledAnnotation() {
		for (Method method : this.getClass().getDeclaredMethods()) {
			if (method.isAnnotationPresent(Scheduled.class)) {
				return method.getAnnotation(Scheduled.class);
			}
		}
		return null;
	}

}
