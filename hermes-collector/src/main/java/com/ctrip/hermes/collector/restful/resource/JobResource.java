package com.ctrip.hermes.collector.restful.resource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.job.AbstractJob;
import com.ctrip.hermes.collector.job.Job;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.restful.utils.RestException;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;

@Component
@Path("/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class JobResource implements ApplicationContextAware {
	private static final Logger LOGGER = LoggerFactory.getLogger(JobResource.class);
	private ApplicationContext m_applicationContext;
	private Map<JobGroup, Map<String, Class<? extends Job>>> m_jobs = new HashMap<>();
	private ExecutorService m_executor = Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(JobResource.class.getSimpleName()));
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	@PostConstruct
	protected void init() {
		Map<String, Job> jobBeans = this.m_applicationContext.getBeansOfType(Job.class);
		
		for (Job job : jobBeans.values()) {
			String jobName = job.getClass().getSimpleName();
			JobGroup jobGroup = JobGroup.DEFAULT;
			JobDescription jobDescription = job.getClass().getAnnotation(JobDescription.class);
			if (jobDescription != null) {
				if (!jobDescription.name().equals("")) {
					jobName = jobDescription.name();
				}
				jobGroup = jobDescription.group();
			}
			
			if (!m_jobs.containsKey(jobGroup)) {
				m_jobs.put(jobGroup, new HashMap<String, Class<? extends Job>>());
			}
			m_jobs.get(jobGroup).put(jobName, job.getClass());
		}
	}
	
	
	@GET
	public Response listJobs() {
		return Response.status(Status.OK).entity(m_jobs).build();
	}
	
	@POST
	public Response triggerJob(@QueryParam("group") String jobGroup, @QueryParam("name") String jobName, @QueryParam("token") String token) {
		if (jobGroup == null || jobName == null || token == null) {
			throw new RestException("Parameters group/name/token are required.", Status.BAD_REQUEST);
		}
		
		if (!token.equals(m_conf.getJobManualToken())) {
			throw new RestException("Token is not valid.", Status.BAD_REQUEST);
		}
		
		JobGroup group = null;
		
		for (JobGroup g : JobGroup.values()) {
			if (g.getName().equals(jobGroup)) {
				group = g;
				break;
			}
		}
		
		if (group == null) {
			throw new RestException(String.format("Group '%s' is not valid! Candidates are %s", jobGroup, StringUtils.join(JobGroup.values(), ", ")), Status.BAD_REQUEST);
		}
		
		Class<? extends Job> jobClass = null;
		
		if (m_jobs.containsKey(group)) {
			jobClass = m_jobs.get(group).get(jobName);
		}
		
		if (jobClass == null) {
			throw new RestException(String.format("Job not found: %s, %s", jobGroup, jobName), Status.BAD_REQUEST);
		}
		
		final Job job = this.m_applicationContext.getBean(jobClass);

		m_executor.submit(new Runnable() {
			
			@Override
			public void run() {
				LOGGER.info("Manually trigger job start: {}", job.getClass());
				((AbstractJob)job).run(null);
				LOGGER.info("Manually trigger job end: {}", job.getClass());
			}
			
		});
		
		return Response.status(Status.OK).entity("ok").build();
	}
	

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.m_applicationContext = applicationContext;
	}
}
