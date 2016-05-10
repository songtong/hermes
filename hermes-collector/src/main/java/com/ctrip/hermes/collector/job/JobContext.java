package com.ctrip.hermes.collector.job;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.SimpleTriggerContext;

import com.ctrip.hermes.collector.job.strategy.ExecutionStrategy;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.state.State;

/**
 * @author tenglinxiao
 *
 */
public class JobContext {
	public static final String JOB_IDENTIFIER = "__JOB_IDENTIFIER__";
	public static final String JOB_CONTEXT_DATA = "__JOB_CONTEXT_DATA__";
	public static final String JOB_STATUS_ID = "__JOB_STATUS_ID__";

	private String m_name;
	private JobGroup m_group;
	private ExecutionStrategy m_executionStrategy;
	// Whether last execution is failed.
	private boolean m_lastExecutionFailed;
	// Current job scheduled time.
	private Date m_scheduledExecutionTime;
	private boolean m_succeed;
	// Trigger for job execution.
	private Trigger m_trigger;
	private List<State> m_states;
	private Record<?> m_record;
	private int m_retries;
	private long m_retryDelay;
	// Last execution time.
	private Date m_lastScheduledExecutionTime;
	// Job context data.
	private Map<String, Object> m_data = new HashMap<String, Object>();
	
	public JobContext() {}

	public String getName() {
		return m_name;
	}

	public void setName(String name) {
		m_name = name;
	}

	public JobGroup getGroup() {
		return m_group;
	}

	public void setGroup(JobGroup group) {
		m_group = group;
	}

	public boolean isSucceed() {
		return m_succeed;
	}

	public void setSucceed(boolean succeed) {
		m_succeed = succeed;
	}
	
	@JsonIgnore
	public List<State> getStates() {
		return m_states;
	}

	public void setStates(List<State> states) {
		this.m_states = states;
	}

	@JsonIgnore
	public Record<?> getRecord() {
		return m_record;
	}

	public void setRecord(Record<?> record) {
		m_record = record;
	}

	public Map<String, Object> getData() {
		return m_data;
	}

	public void setData(Map<String, Object> data) {
		m_data = data;
	}
	
	public Date getLastScheduledExecutionTime() {
		return m_lastScheduledExecutionTime;
	}

	public void setLastScheduledExecutionTime(Date lastScheduledExecutionTime) {
		m_lastScheduledExecutionTime = lastScheduledExecutionTime;
	}
	
	@JsonIgnore
	public Trigger getTrigger() {
		return m_trigger;
	}

	public void setTrigger(Trigger trigger) {
		m_trigger = trigger;
	}
	
	public Date getScheduledExecutionTime() {
		return m_scheduledExecutionTime;
	}

	public void setScheduledExecutionTime(Date scheduledExecutionTime) {
		m_scheduledExecutionTime = scheduledExecutionTime;
	}

	@JsonIgnore
	public ExecutionStrategy getExecutionStrategy() {
		return m_executionStrategy;
	}

	public void setExecutionStrategy(ExecutionStrategy executionStrategy) {
		m_executionStrategy = executionStrategy;
	}

	public long getRetryDelay() {
		return m_retryDelay;
	}

	public void setRetryDelay(long retryDelay) {
		m_retryDelay = retryDelay;
	}

	public boolean hasMissedJobs() {
		return nextScheduledExecutionTime().getTime() <= System.currentTimeMillis();
	}
	
	public Date nextScheduledExecutionTime() {
		SimpleTriggerContext triggerContext = new SimpleTriggerContext(m_scheduledExecutionTime, m_scheduledExecutionTime, m_scheduledExecutionTime);
		return m_trigger.nextExecutionTime(triggerContext);
	}
	
	public boolean isLastExecutionFailed() {
		return m_lastExecutionFailed;
	}

	public void setLastExecutionFailed(boolean lastExecutionFailed) {
		m_lastExecutionFailed = lastExecutionFailed;
	}

	public boolean commitable() {
		return this.m_succeed && this.m_states != null && this.m_states.size() > 0;
	}
	
	public int getRetries() {
		return m_retries;
	}

	public void setRetries(int retries) {
		m_retries = retries;
	}
	
	public boolean retry() {
		return m_retries-- > 0;
	}

	public Object clone() {
		JobContext context = new JobContext();
		context.setName(this.m_name);
		context.setGroup(this.m_group);
		context.setLastExecutionFailed(this.m_lastExecutionFailed);
		context.setExecutionStrategy(this.m_executionStrategy);
		context.setLastScheduledExecutionTime(this.m_lastScheduledExecutionTime);
		context.setScheduledExecutionTime(this.m_scheduledExecutionTime);
		context.setRetries(this.m_retries);
		context.setRetryDelay(this.m_retryDelay);
		context.setTrigger(m_trigger);
		return context;
	}
}
