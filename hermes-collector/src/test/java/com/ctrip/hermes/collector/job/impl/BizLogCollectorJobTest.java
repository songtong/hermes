package com.ctrip.hermes.collector.job.impl;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContext;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContextBuilder;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.exception.DataNotReadyException;
import com.ctrip.hermes.collector.job.CollectorJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.utils.IndexUtils;
import com.ctrip.hermes.collector.utils.TimeUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class BizLogCollectorJobTest {
	@Autowired
	private BizLogCollectorJob m_collectorJob;
	
	@Autowired
	private DatasourceManager m_manager;
	
	@Test(expected=DataNotReadyException.class)
	public void testDoRun1() throws Exception {
		JobContext context = new JobContext();
		context.setGroup(JobGroup.DEFAULT);
		context.setName("test");
		EsHttpCollectorContextBuilder builder = EsHttpCollectorContextBuilder.newContextBuilder((EsDatasource)m_manager.getDefaultDatasource(HttpDatasourceType.ES), RecordType.TOPIC_FLOW);
		long from = System.currentTimeMillis();
		long to = TimeUtils.after(from, 10, TimeUnit.MINUTES);
		builder.timeRange(from, to);
		builder.indices(IndexUtils.getBizIndex(from));
		EsHttpCollectorContext ctx = builder.build();
		
		context.getData().put(CollectorJob.COLLECTOR_CONTEXT, ctx);
		m_collectorJob.doRun(context);
	}
		
	@Test
	public void testDoRun2() throws Exception {
		JobContext context = new JobContext();
		context.setGroup(JobGroup.DEFAULT);
		context.setName("test");
		EsHttpCollectorContextBuilder builder = EsHttpCollectorContextBuilder.newContextBuilder((EsDatasource)m_manager.getDefaultDatasource(HttpDatasourceType.ES), RecordType.TOPIC_FLOW);
		long to = TimeUtils.before(System.currentTimeMillis(), 10, TimeUnit.MINUTES);
		long from = TimeUtils.before(to, 10, TimeUnit.MINUTES);
		builder.timeRange(from, to);
		builder.indices(IndexUtils.getBizIndex(from));
		EsHttpCollectorContext ctx = builder.build();
		
		context.getData().put(CollectorJob.COLLECTOR_CONTEXT, ctx);
		Assert.assertTrue(m_collectorJob.doRun(context));
	}
	
	public void testSuccess() {
		
	}
}
