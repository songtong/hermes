package com.ctrip.hermes.collector.job.impl;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContext;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.exception.DataNotReadyException;
import com.ctrip.hermes.collector.hub.NotifierManager;
import com.ctrip.hermes.collector.job.CollectorJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.job.annotation.JobStrategy;
import com.ctrip.hermes.collector.job.strategy.DelayedRescheduleExecutionStrategy;
import com.ctrip.hermes.collector.notice.impl.PotentialIssueNotice;
import com.ctrip.hermes.collector.service.EsHttpCollectorService;
import com.ctrip.hermes.collector.utils.TimeUtils;

@Component
@JobDescription(group=JobGroup.BIZ, cron="0 */5 * * * ?")
@JobStrategy(DelayedRescheduleExecutionStrategy.class)
public class BizLogCollectorJob extends CollectorJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(BizLogCollectorJob.class);
	
	@Autowired
	private DatasourceManager m_datasourceManager;
	
	@Autowired
	private NotifierManager m_notifierManager;
	
	@Autowired
	private EsHttpCollectorService m_esHttpCollectorService;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	public boolean doRun(JobContext context) throws Exception {
		EsHttpCollectorContext ctx = (EsHttpCollectorContext)context.getData().get(COLLECTOR_CONTEXT);
		
		long maxTimestamp = -1;
		long upboundaryTimestamp = ctx.getTo();
		
		CollectorContext maxDateQueryContext = m_esHttpCollectorService.createQueryContextForMaxDate((EsDatasource)ctx.getDatasource(), ctx.getFrom(), ctx.getTo(), ctx.getUrl());
		if (collect(context, maxDateQueryContext)) {
			maxTimestamp = m_esHttpCollectorService.getMaxTimestampFromResponse(context.getRecord());
			if (maxTimestamp >= upboundaryTimestamp) {
				return super.doRun(context);
			}
		}
		
		throw new DataNotReadyException(String.format("Es data not ready: max-date is %s and required-date is up to %s", maxTimestamp, upboundaryTimestamp));
	}

	public CollectorContext createContext(JobContext context) {
		long to = TimeUtils.before(context.getScheduledExecutionTime().getTime(), 5, TimeUnit.MINUTES);
		long from = TimeUtils.before(to, 5, TimeUnit.MINUTES);

		EsDatasource datasource = (EsDatasource) m_datasourceManager.getDefaultDatasource(HttpDatasourceType.ES);
		return m_esHttpCollectorService.createQueryContextForTopicFlow(datasource, from, to);
	}
	
	@Override
	public void success(JobContext context) {
		context.setStates(m_esHttpCollectorService.getTopicFlowStatesFromResponse(context.getRecord()));		
		if (context.getStates() != null && context.getStates().size() == 0) {
			notifyPotentialIssues();
		}
	}
		
	private void notifyPotentialIssues() {
		PotentialIssueNotice noticeContent = new PotentialIssueNotice();
		noticeContent.setMessage("Hermes-biz index is prone to encouter problems because there is no data after biz data fetch!");
		LOGGER.warn(noticeContent.getMessage());
		m_notifierManager.offer(new HermesNotice(Arrays.asList(m_conf.getNotifierDefaultMail().split(",")), noticeContent), null);
	}
}
