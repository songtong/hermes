package com.ctrip.hermes.collector.job.impl;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.job.Job;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.job.annotation.JobStrategy;
import com.ctrip.hermes.collector.job.strategy.ExecutionStrategy;
import com.ctrip.hermes.collector.job.strategy.NoRetryExecutionStrategy;
import com.ctrip.hermes.collector.state.State;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CollectorTest.class)
public class BrokerCommandDropCollectorJobTest implements ApplicationContextAware {
	@Component
	private static class MockedBrokerCommandDropCollectorJob extends BrokerCommandDropCollectorJob {
		
		public CollectorContext createContext(JobContext context) {
			context.setScheduledExecutionTime(new Date());
			return super.createContext(context);
		}

		public boolean doRun(JobContext context) throws Exception {
			boolean result = super.doRun(context);
			for (State state : context.getStates()) {
				System.out.println(state);
			}
			return result;
		}
	}

	@Autowired
	private MockedBrokerCommandDropCollectorJob m_job;
	
	private ApplicationContext m_applicationContext;

	private JobContext createJobContext(Job job) {
		JobContext context = createDefaultJobContext(job);
		JobStrategy strategy = job.getClass().getSuperclass().getAnnotation(JobStrategy.class);
		Class<? extends ExecutionStrategy> strategyClazz = NoRetryExecutionStrategy.class;
		if (strategy != null) {
			strategyClazz = strategy.value();
			context.setRetries(strategy.retries());
			context.setRetryDelay(strategy.retryDelay());
		}

		context.setExecutionStrategy((ExecutionStrategy) findExactMatch(strategyClazz));
		return context;
	}

	private JobContext createDefaultJobContext(Job job) {
		JobContext context = new JobContext();
		// Default
		context.setName(job.getClass().getSuperclass().getSimpleName());
		context.setGroup(JobGroup.DEFAULT);

		if (job.getClass().getSuperclass().isAnnotationPresent(JobDescription.class)) {
			JobDescription identifier = job.getClass().getSuperclass().getAnnotation(
					JobDescription.class);
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
				return (T) bean;
			}
		}
		return null;
	}
	
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.m_applicationContext = applicationContext;
	}

	@Test
	public void testRun() throws IOException {
		m_job.run(createJobContext(m_job));
		System.in.read();
	}

}
