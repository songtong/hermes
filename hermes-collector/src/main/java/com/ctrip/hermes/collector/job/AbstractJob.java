package com.ctrip.hermes.collector.job;

import java.util.Calendar;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.dal.job.JobStatus;
import com.ctrip.hermes.collector.hub.StateHub;
import com.ctrip.hermes.collector.service.JobStatusService;
import com.ctrip.hermes.collector.state.State;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

/**
 * @author tenglinxiao
 *
 */
public abstract class AbstractJob implements Job, ApplicationContextAware {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJob.class);
	private ApplicationContext m_applicationContext;

	@Autowired
	protected CollectorConfiguration m_conf;
	
	@Autowired
	protected StateHub m_stateHub;
	
	@Autowired
	protected JobStatusService jobStatusService;
	
	public boolean run(JobContext context) {
		if (context == null) {
			LOGGER.error("Job can NOT run without job context.");
			return false;
		}
		
		LOGGER.info("Job kickoff: {}, {}", context.getName(), context.getGroup());

		Transaction transaction = Cat.newTransaction("Job", String.format("%s-%s", context.getName(), context.getGroup()));
		JobStatus jobStatus = null;
		String message = null;
		try {
			if (m_conf.isEnableLogJobStatus()) {
				// Find or create job status.
				jobStatus = jobStatusService.findOrCreateJobStatus(context);
				jobStatus.setScheduleTime(context.getScheduledExecutionTime());
				jobStatus.setStartTime(Calendar.getInstance().getTime());
				jobStatus.setEndTime(null);
				jobStatus.setStatus(Status.RUNNING.toString());
	
				// Update start time & status.
				jobStatusService.updateJobStatus(jobStatus);
			}
			
			setup(context);
			
			if (doRun(context)) {
				context.setSucceed(true);
				transaction.setStatus(Message.SUCCESS);
				LOGGER.info("Job run success: {}, {}", context.getName(), context.getGroup());
			} else {
				LOGGER.info("Job run failed: {}, {}", context.getName(), context.getGroup());
			}
		} catch (Exception e) {
			message = e.getMessage();
			transaction.setStatus(e);
			LOGGER.error("Job failed due to exception: {}, {}", context.getName(), context.getGroup(), e);
		} finally {
			complete(context);
			transaction.complete();
			if (m_conf.isEnableLogJobStatus() && jobStatus != null) {
				try {
					Status status = context.isSucceed()? Status.SUCCESS: Status.FAILED;
					jobStatus.setStatus(status.toString());
					jobStatus.setEndTime(Calendar.getInstance().getTime());
					// Update status & end time.
					jobStatusService.updateJobStatus(jobStatus);
					jobStatusService.logJobStatus(jobStatus, message);
				} catch (DalException e) {
					LOGGER.error("Failed to update job status in db due to exception: {}", e.getMessage(), e);
				}
			}
		}
		
		LOGGER.info("Job done with status [{}]: {}, {}", context.isSucceed()? Status.SUCCESS: Status.FAILED, context.getName(), context.getGroup());
		return context.isSucceed();
	}
	
	protected abstract void setup(JobContext ctx);
	
	protected abstract boolean doRun(JobContext ctx) throws Exception;
	
	// Method called no matter the job failed or succeed.
	protected void complete(JobContext context) {
		// Only if the whole workflow is succeed and there is record/state available, then submit the record/state.
		if (context.commitable()) {
			commitStates(context.getStates());
		}
	}
	
	protected void commitStates(List<State> states) {
		for (State state : states) {
			m_stateHub.offer(state);
		}
		states.clear();
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.m_applicationContext = applicationContext;
	}
	
	public ApplicationContext getApplicationContext() {
		return m_applicationContext;
	}
}
