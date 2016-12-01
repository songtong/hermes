package com.ctrip.hermes.collector.job.impl;

import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.job.Job;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.job.annotation.JobStrategy;
import com.ctrip.hermes.collector.job.strategy.ExecutionStrategy;
import com.ctrip.hermes.collector.job.strategy.NoRetryExecutionStrategy;
import com.ctrip.hermes.collector.service.EsHttpCollectorService;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.utils.TimeUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CollectorTest.class)
public class MetaServerErrorCollectorJobTest implements ApplicationContextAware {
	@Component
	private static class MockedMetaServerErrorCollectorJob extends MetaServerErrorCollectorJob {
		@Autowired
		private DatasourceManager m_datasourceManager;

		@Autowired
		private EsHttpCollectorService m_esHttpCollectorService;

		public CollectorContext createContext(JobContext context) {
			long to = System.currentTimeMillis();
			long from = TimeUtils.before(to, 1, TimeUnit.DAYS);
			EsDatasource datasource = (EsDatasource) m_datasourceManager
					.getDefaultDatasource(HttpDatasourceType.ES);
			return m_esHttpCollectorService.createQueryContextForBrokerError(
					datasource, from, to);
		}

		public void success(JobContext context) {
			super.success(context);
			for (State state : context.getStates()) {
				System.out.println(state);
			}
		}
	}

	@Autowired
	private MockedMetaServerErrorCollectorJob m_job;
	
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
	public void testRun() {
		m_job.run(createJobContext(m_job));
	}

	@Test
	public void testQueryString() {
		// long to = System.currentTimeMillis();
		// long from = TimeUtils.before(to, 1, TimeUnit.DAYS);
		//
		// EsHttpCollectorContextBuilder contextBuilder =
		// EsHttpCollectorContextBuilder
		// .newContextBuilder(null, RecordType.BROKER_ERROR);
		// contextBuilder.timeRange(from, to);
		//
		// contextBuilder
		// .queryBuilder(QueryBuilders
		// .boolQuery()
		// .must(QueryBuilders.rangeQuery("@timestamp").from(from)
		// .to(to))
		// .must(QueryBuilders.termQuery("source", "broker"))
		// .must(QueryBuilders.termQuery("message", "error"))
		// .mustNot(
		// QueryBuilders
		// .boolQuery()
		// .must(QueryBuilders.termsQuery(
		// "hostname", "VMS21313",
		// "VMS21380", "VMS12602"))
		// .must(EsHttpCollectorService.generateOrPhraseQuery(
		// "message",
		// EsHttpCollectorService.toArray("[\"NOT_LEADER_FOR_PARTITION\",\"Failed to append messages\"]")))));
		// contextBuilder.querySize(1000);
	}
}
