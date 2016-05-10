package com.ctrip.hermes.collector.job.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryFilterBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContextBuilder;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.job.CollectorJob;
import com.ctrip.hermes.collector.job.JobContext;
import com.ctrip.hermes.collector.job.JobGroup;
import com.ctrip.hermes.collector.job.annotation.JobDescription;
import com.ctrip.hermes.collector.job.annotation.JobStrategy;
import com.ctrip.hermes.collector.job.strategy.RetryUtilSuccessExecutionStrategy;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.service.EsHttpCollectorService;
import com.ctrip.hermes.collector.state.impl.ConsumeFlowState;
import com.ctrip.hermes.collector.state.impl.ProduceFlowState;
import com.ctrip.hermes.collector.utils.JsonSerializer;
import com.ctrip.hermes.collector.utils.TimeUtils;

@Component
@JobDescription(group=JobGroup.REPORT, cron="0 0 1 * * ?")
@JobStrategy(RetryUtilSuccessExecutionStrategy.class)
public class BizLogDailyReportJob extends CollectorJob {	
	@Autowired
	private DatasourceManager m_datasourceManager;
	
	@Autowired
	private EsHttpCollectorService m_esHttpCollectorService;

	@Override
	public CollectorContext createContext(JobContext context) {
		long to = TimeUtils.correctTime(context.getScheduledExecutionTime(), TimeUnit.DAYS);
		long from = TimeUtils.before(to, 1, TimeUnit.DAYS) + 1;

		EsDatasource datasource = (EsDatasource) m_datasourceManager.getDefaultDatasource(HttpDatasourceType.ES);
		return m_esHttpCollectorService.createQueryContextForTopicFlowDailyReport(datasource, from, to);
	}

	@Override
	public void success(JobContext context) throws Exception {
		context.setStates(m_esHttpCollectorService.getDailyReportStateFromResponse(context.getRecord()));
	}
	
	public static void main(String[] args) {
		long to = 0;
		try {
			to = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2016-06-01 01:00:00").getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long from = TimeUtils.before(to, 1, TimeUnit.HOURS);

		EsHttpCollectorContextBuilder contextBuilder = EsHttpCollectorContextBuilder.newContextBuilder(null, RecordType.TOPIC_FLOW_DAILY);
		contextBuilder.timeRange(from, to);
		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("@timestamp").from(from).to(to);
		contextBuilder.queryBuilder(queryBuilder);

		// Create aggregation query for topic-producer.
		QueryFilterBuilder queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders
				.queryStringQuery(String.format("%s:%s", JsonSerializer.TYPE, ProduceFlowState.class.getName())));
		FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("produces").filter(
				queryFilterBuilder);
		filterAggregationBuilder.subAggregation(AggregationBuilders.sum("total").field("count")).subAggregation(
				AggregationBuilders.terms("topic").field("topicId").size(0).order(Order.aggregation("sum", false))
						.subAggregation(AggregationBuilders.sum("sum").field("count")));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		// Create aggregation query for topic-consume.
		queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders
				.queryStringQuery(String.format("%s:%s", JsonSerializer.TYPE, ConsumeFlowState.class.getName())));
		filterAggregationBuilder = AggregationBuilders.filter("consumes").filter(queryFilterBuilder);
		filterAggregationBuilder.subAggregation(AggregationBuilders.sum("total").field("count")).subAggregation(
				AggregationBuilders.terms("topic").field("topicId").size(0).order(Order.aggregation("sum", false))
						.subAggregation(AggregationBuilders.sum("sum").field("count")));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		contextBuilder.aggregationBuilders(AggregationBuilders.terms("topic").field("topicId").size(0));

		System.out.println(JSON.toJSONString(contextBuilder.build().getData()));
	}

}
