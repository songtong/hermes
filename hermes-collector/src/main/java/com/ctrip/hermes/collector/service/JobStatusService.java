package com.ctrip.hermes.collector.service;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.ctrip.hermes.collector.dal.job.JobHistory;
import com.ctrip.hermes.collector.dal.job.JobHistoryDao;
import com.ctrip.hermes.collector.dal.job.JobStatus;
import com.ctrip.hermes.collector.dal.job.JobStatusDao;
import com.ctrip.hermes.collector.dal.job.JobStatusEntity;
import com.ctrip.hermes.collector.job.Job.Status;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

@Component
public class JobStatusService {
	private JobStatusDao m_jobStatusDao = PlexusComponentLocator
			.lookup(JobStatusDao.class);

	private JobHistoryDao m_jobHistoryDao = PlexusComponentLocator
			.lookup(JobHistoryDao.class);

	public JobStatus findOrCreateJobStatus(JobContext context)
			throws DalException {
		JobStatus jobStatus = null;
		try {
			jobStatus = m_jobStatusDao.findByNameGroup(context.getName(),
					context.getGroup().getName(), JobStatusEntity.READSET_FULL);
		} catch (DalNotFoundException e) {
			jobStatus = new JobStatus();
			jobStatus.setName(context.getName());
			jobStatus.setGroup(context.getGroup().getName());
			jobStatus.setStatus(Status.INITIAL.toString());
			m_jobStatusDao.insert(jobStatus);
		}

		return jobStatus;
	}

	public void updateJobStatus(JobStatus jobStatus) throws DalException {
		m_jobStatusDao.updateByPK(jobStatus, JobStatusEntity.UPDATESET_FULL);
	}

	public void logJobStatus(JobStatus jobStatus, String message)
			throws DalException {
		JobHistory jobHistory = new JobHistory();
		jobHistory.setName(jobStatus.getName());
		jobHistory.setGroup(jobStatus.getGroup());
		jobHistory.setStatus(jobStatus.getStatus());
		jobHistory.setScheduleTime(jobStatus.getScheduleTime());
		jobHistory.setStartTime(jobStatus.getStartTime());
		jobHistory.setEndTime(jobStatus.getEndTime());
		if (message != null) {
			jobHistory.setMessage(message.getBytes());
		}
		m_jobHistoryDao.insert(jobHistory);
	}

	public void saveExecutionContext(JobContext context) throws Exception {
		JobStatus status = m_jobStatusDao.findByNameGroup(context.getName(),
				context.getGroup().getName(), JobStatusEntity.READSET_FULL);
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
		status.setContext(mapper.writeValueAsString(context.getData()));
		m_jobStatusDao.updateByPK(status, JobStatusEntity.UPDATESET_FULL);
	}

	public void readExecutionContext(JobContext context) throws Exception {
		// Make sure job status exists.
		JobStatus status = findOrCreateJobStatus(context);

		context.setLastScheduledExecutionTime(status.getScheduleTime());
		context.setScheduledExecutionTime(status.getScheduleTime());

		if (Status.valueOf(status.getStatus()) == Status.SUCCESS) {
			context.setScheduledExecutionTime(context
					.nextScheduledExecutionTime());
		}

		if (status.getContext() != null) {
			ObjectMapper mapper = new ObjectMapper();
			Map<String, Object> data = mapper.readValue(status.getContext(),
					Map.class);
			context.setData(data);
		}
	}
}
