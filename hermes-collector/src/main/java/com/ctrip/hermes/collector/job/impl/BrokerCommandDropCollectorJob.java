package com.ctrip.hermes.collector.job.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.CatHttpCollector.CatHttpCollectorContext;
import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.HttpDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.job.CollectorJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.job.annotation.JobStrategy;
import com.ctrip.hermes.collector.job.strategy.RetryExecutionStrategy;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.service.CatHttpCollectorService;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.impl.CommandDropState;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;
import com.ctrip.hermes.collector.utils.Constants;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.model.Endpoint;
import com.ctrip.hermes.metaservice.model.EndpointDao;
import com.ctrip.hermes.metaservice.model.EndpointEntity;

@Component
@JobDescription(group=JobGroup.BIZ, cron="0 */5 * * * ?")
@JobStrategy(RetryExecutionStrategy.class)
public class BrokerCommandDropCollectorJob extends CollectorJob {
	private static Logger LOGGER = LoggerFactory.getLogger(BrokerCommandDropCollectorJob.class);
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	@Autowired
	private DatasourceManager m_datasourceManager;
	
	@Autowired
	private CatHttpCollectorService m_catHttpCollectorService;
	
	private EndpointDao m_endpointDao = PlexusComponentLocator.lookup(EndpointDao.class);
	
	private ExecutorService m_executor = Executors.newFixedThreadPool(5, CollectorThreadFactory.newFactory(BrokerCommandDropCollectorJob.class));
	
	private AtomicInteger m_succeeds = new AtomicInteger(0);
	
	@Override
	public CollectorContext createContext(JobContext context) {	
		return null;
	}
	
	public CollectorContext createContext(JobContext context, String host) {	
		long to = context.getScheduledExecutionTime().getTime();
		long from = TimeUtils.before(to, 5, TimeUnit.MINUTES);
		CatHttpCollectorContext ctx = new CatHttpCollectorContext((HttpDatasource)m_datasourceManager.getDefaultDatasource(HttpDatasourceType.CAT), RecordType.COMANND_DROP);
		
		ctx.setFrom(from);
		ctx.setTo(to);
		ctx.setUrl(String.format(m_conf.getCatEventApiPattern(), Constants.DOMAIN_BROKER, TimeUtils.formatCatTimestamp(from), host, CatConstants.TYPE_CMD_DROP));
		
		return ctx;
	}

	// Actually run the job.
	public boolean doRun(final JobContext context) throws Exception {
		try {
			List<Endpoint> endpoints = m_endpointDao.list(EndpointEntity.READSET_FULL);
			final List<State> states = Collections.synchronizedList(new ArrayList<State>());
			final CountDownLatch latch = new CountDownLatch(endpoints.size());
			for (final Endpoint endpoint : endpoints) {
				m_executor.submit(new Runnable() {
					public void run() {
						try {
							JobContext newContext = (JobContext)context.clone();
							
							if (collect(newContext, createContext(newContext, endpoint.getHost()))) {
								m_succeeds.incrementAndGet();
								success(newContext);
								if (newContext.commitable()) {
									for (State state : newContext.getStates()) {
										((CommandDropState)state).setHost(endpoint.getHost());
									}
									states.addAll(newContext.getStates());
								}
							} else {
								fail(newContext);
							}
						} catch (Exception e) {
							LOGGER.error("Failed to fetch command drop data from CAT for broker {}.", endpoint.getHost(), e);
						} finally {
							latch.countDown();
						}
					}
				});
			}
			
			latch.await(5, TimeUnit.MINUTES);
			if (m_succeeds.get() > (1 + endpoints.size()) / 2) {
				context.setStates(states);
				context.setSucceed(true);
			} else {
				context.setSucceed(false);
			}
		} finally {
			context.getData().remove(COLLECTOR_CONTEXT);
		}
		return context.isSucceed();
	}
	
	@Override
	public void success(JobContext context) throws Exception {
	    if (context.getRecord() != null) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(TimeUtils.before(context.getScheduledExecutionTime().getTime(), 5, TimeUnit.MINUTES));
            int fromMinute = calendar.get(Calendar.MINUTE);
    		context.setStates(m_catHttpCollectorService.getBrokerCommandDropStatesFromResponse(context.getRecord(), fromMinute, fromMinute + 5));
	    }
	}

}
