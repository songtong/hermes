package com.ctrip.hermes.collector.job.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.job.CollectorJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.job.annotation.JobStrategy;
import com.ctrip.hermes.collector.job.strategy.RetryUtilSuccessExecutionStrategy;
import com.ctrip.hermes.collector.service.EsHttpCollectorService;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.utils.TimeUtils;

@Component
@JobDescription(group = JobGroup.BIZ, cron="0 30 0 * * ?")
@JobStrategy(RetryUtilSuccessExecutionStrategy.class)
public class BizLogDailyAggregationJob extends CollectorJob {

	@Autowired
	private DatasourceManager m_datasourceManager;
	
	@Autowired
	private EsHttpCollectorService m_esHttpCollectorService;

	@Override
	public CollectorContext createContext(JobContext context) {
		long to = TimeUtils.correctTime(context.getScheduledExecutionTime(), TimeUnit.DAYS);
		long from = TimeUtils.before(to, 1, TimeUnit.DAYS);

		EsDatasource datasource = (EsDatasource) m_datasourceManager.getDefaultDatasource(HttpDatasourceType.ES);
		return m_esHttpCollectorService.createQueryContextForTopicFlowAggregation(datasource, from, to);
	}

	@Override
	public void success(JobContext context) {
		List<State> states = m_esHttpCollectorService.getTopicFlowAggregationStatesFromResponse(context.getRecord());
		context.setStates(states);
	}
}
