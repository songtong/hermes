package com.ctrip.hermes.collector.job.impl;

import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import com.ctrip.hermes.collector.job.strategy.DelayedRescheduleExecutionStrategy;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.service.CatHttpCollectorService;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.impl.ProduceLatencyState;
import com.ctrip.hermes.collector.utils.Constants;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.core.constants.CatConstants;

@Component
@JobDescription(group=JobGroup.BIZ, cron="0 */5 * * * ?")
@JobStrategy(DelayedRescheduleExecutionStrategy.class)
public class ProduceLargeLatencyCollectorJob extends CollectorJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProduceLargeLatencyCollectorJob.class);
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	@Autowired
	private DatasourceManager m_datasourceManager;
	
	@Autowired
	private CatHttpCollectorService m_catHttpCollectorService;

	@Override
	public CollectorContext createContext(JobContext context) {	
		long to = context.getScheduledExecutionTime().getTime();
		long from = TimeUtils.before(to, 5, TimeUnit.MINUTES);
		CatHttpCollectorContext ctx = new CatHttpCollectorContext((HttpDatasource)m_datasourceManager.getDefaultDatasource(HttpDatasourceType.CAT), RecordType.PRODUCE_LATENCY);
		
		ctx.setFrom(from);
		ctx.setTo(to);
		ctx.setUrl(String.format(m_conf.getCatTransactionApiPattern(), Constants.DOMAIN_ALL, TimeUtils.formatCatTimestamp(from), CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE_LARGE));
		
		return ctx;
	}
	
	public boolean doRun(JobContext context) throws Exception {
		if (super.doRun(context)) {
			CatHttpCollectorContext ctx = (CatHttpCollectorContext)createContext(context);
			ctx.setUrl(String.format(m_conf.getCatTransactionApiPattern(), Constants.DOMAIN_ALL, TimeUtils.formatCatTimestamp(ctx.getFrom()), CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE));
			if (super.collect(context, ctx)) {
				Map<String, Long> elapses = m_catHttpCollectorService.getProduceElapseStatesFromResponse(context.getRecord());
				for (State state : context.getStates()) {
					ProduceLatencyState s = (ProduceLatencyState)state;
					Long count = elapses.get(s.getTopic());
					if (count == null) {
						count = 0L;
						LOGGER.warn("No normal latency records for topic {}, large elapse records number is {}", s.getTopic(), s.getCount());
					}
					s.setCountAll(s.getCount() + count);
					s.setRatio((double)s.getCount() / s.getCountAll());
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public void success(JobContext context) throws Exception {
	    if (context.getRecord() != null) {
    		context.setStates(m_catHttpCollectorService.getProduceLatencyStatesFromRespone(context.getRecord()));
	    }
	}
	
//	private int getMinute(long timestamp) {
//		Calendar calendar = Calendar.getInstance();
// 	    calendar.setTimeInMillis(timestamp);
// 	    return calendar.get(Calendar.MINUTE);
//	}
}
