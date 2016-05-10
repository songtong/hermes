package com.ctrip.hermes.collector.datasource;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;

public class Test {
	public static void main(String args[]) {
		SearchRequestBuilder searchRequest = new TransportClient().prepareSearch("index");
		FilterBuilder filter = FilterBuilders.andFilter(FilterBuilders.queryFilter(QueryBuilders.queryStringQuery("a:b")));
		FilterAggregationBuilder builder = AggregationBuilders.filter("filter").filter(filter);
		SumBuilder sum = AggregationBuilders.sum("sum");
		//ChildrenBuilder childrenBuilder = AggregationBuilders..children("aggregation1").subAggregation(builder);
		searchRequest.addAggregation(sum);
		searchRequest.addAggregation(builder);
		System.out.println(searchRequest.toString());
	}
}
