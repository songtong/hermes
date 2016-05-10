package com.ctrip.hermes.collector.job;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryFilterBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContext;
import com.ctrip.hermes.collector.collector.EsQueryContextBuilder;
import com.ctrip.hermes.collector.collector.EsQueryContextBuilder.EsQueryType;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.job.annotation.Job;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.utils.TimeUtils;

@Component
@Job(group=JobGroup.BIZ)
public class DailyFlowMonitorJob extends ScheduledCollectorJob {
	@Autowired
	private DatasourceManager m_datasourceManager;
	
	public AggregationBuilder createAggregationBuilder(String... fields) {
		//AggregationBuilders.children("").filter("").filter(filter)
		TermsBuilder termsBuilder = null; 
		TermsBuilder subTermsBuilder = null;
		for (String field : fields) {
			if (subTermsBuilder == null) {
				subTermsBuilder = AggregationBuilders.terms("group");
				termsBuilder = subTermsBuilder;
			} else {
				TermsBuilder newTermsBuilder = AggregationBuilders.terms("group");
				subTermsBuilder.subAggregation(newTermsBuilder);
				subTermsBuilder = newTermsBuilder;
			}
			subTermsBuilder.field(field);
		}
		return termsBuilder;
	}

	@Override
	public CollectorContext createContext(JobContext context) {
		long to = TimeUtils.correctTime(1, TimeUnit.DAYS);
		long from = TimeUtils.prevTime(to, 1, TimeUnit.DAYS);
		
		EsQueryContextBuilder contextBuilder = EsQueryContextBuilder.newBuilder(EsQueryType.QUERY);
		contextBuilder.timeRange(from, to);
		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("@timestamp").from(from).to(to);
		contextBuilder.queryBuilder(queryBuilder);
		
		// Create aggregation query for topic-producer.
		QueryFilterBuilder queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders.queryStringQuery("source: broker AND eventType: Message.Received"));
		FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("produces").filter(queryFilterBuilder);
		filterAggregationBuilder.subAggregation(createAggregationBuilder("topic", "partition"));
		
		contextBuilder.aggregationBuilders(filterAggregationBuilder);
		
		// Create aggregation query for topic-consumer.
		queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders.queryStringQuery("source: broker AND eventType: Message.Acked"));

		filterAggregationBuilder = AggregationBuilders.filter("consumes").filter(queryFilterBuilder);
		filterAggregationBuilder.subAggregation(createAggregationBuilder("topic", "groupId", "partition"));
		
		contextBuilder.aggregationBuilders(filterAggregationBuilder);
		
		contextBuilder.useDefaultBizIndex();

		EsDatasource datasource = EsDatasource.newEsDatasource(RecordType.TOPIC_FLOW.getName());
		return EsHttpCollectorContext.newContext(datasource, null, contextBuilder.build(), RecordType.TOPIC_FLOW);
	}
	
	@Scheduled(cron="*/30 * * * * ?")
	public void schedule() {
		super.schedule(null);
	}

	@Override
	public void success(JobContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(JobContext context) {
		// TODO Auto-generated method stub
		
	}
}
