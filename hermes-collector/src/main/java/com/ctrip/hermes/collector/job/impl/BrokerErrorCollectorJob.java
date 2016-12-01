package com.ctrip.hermes.collector.job.impl;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.job.CollectorJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.job.annotation.JobStrategy;
import com.ctrip.hermes.collector.job.strategy.DelayedRescheduleExecutionStrategy;
import com.ctrip.hermes.collector.job.strategy.RetryExecutionStrategy;
import com.ctrip.hermes.collector.service.EsHttpCollectorService;
import com.ctrip.hermes.collector.utils.TimeUtils;

@Component
@JobDescription(group = JobGroup.SERVICE, cron="0 */5 * * * ?")
@JobStrategy(DelayedRescheduleExecutionStrategy.class)
public class BrokerErrorCollectorJob extends CollectorJob {
	@Autowired
	private DatasourceManager m_datasourceManager;
	
	@Autowired
	private EsHttpCollectorService m_esHttpCollectorService;

	@Autowired
	private CollectorConfiguration m_conf;
	
	@Override
	public CollectorContext createContext(JobContext context) {
		long to = TimeUtils.before(context.getScheduledExecutionTime().getTime(), 5, TimeUnit.MINUTES);
		long from = TimeUtils.before(to, 5, TimeUnit.MINUTES);

		EsDatasource datasource = (EsDatasource) m_datasourceManager.getDefaultDatasource(HttpDatasourceType.ES);
		return m_esHttpCollectorService.createQueryContextForBrokerError(datasource, from, to);
	}

	@Override
	public void success(JobContext context) {
		context.setStates(m_esHttpCollectorService.getBrokerErrorStateFromResponse(context.getRecord()));
	}
	
}
