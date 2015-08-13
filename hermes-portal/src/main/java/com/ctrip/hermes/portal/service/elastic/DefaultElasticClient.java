package com.ctrip.hermes.portal.service.elastic;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.portal.config.PortalConfig;

@Named(type = ElasticClient.class)
public class DefaultElasticClient implements Initializable, ElasticClient {
	private static final Logger log = LoggerFactory.getLogger(DefaultElasticClient.class);

	private static final String AGG_NAME = "hermes-agg"; // could be any string you like

	@Inject
	private PortalConfig m_config;

	private Client m_eClient;

	private SearchRequestBuilder prepareSearch() {
		SearchRequestBuilder sb = m_eClient.prepareSearch("logstash-*");
		sb.setTypes(m_config.getElasticDocumentType());
		sb.setSearchType("count");
		return sb;
	}

	public Map<String, Integer> getLastMinuteCount(String field, String query, int size) {
		Map<String, Integer> map = new LinkedHashMap<>();

		long _1MinAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1);
		QueryBuilder qb = QueryBuilders.queryStringQuery(query);
		FilterBuilder fb = FilterBuilders.rangeFilter("@timestamp").from(_1MinAgo);
		TermsBuilder tb = new TermsBuilder(AGG_NAME).field(field).order(Order.count(false));
		if (size > 0) {
			tb.size(size);
		}

		SearchRequestBuilder sb = prepareSearch();
		sb.setSearchType(SearchType.COUNT);
		sb.setQuery(QueryBuilders.filteredQuery(qb, fb));
		sb.addAggregation(tb);
		try {
			SearchResponse sr = sb.execute().actionGet();
			if (RestStatus.OK.equals(sr.status())) {
				for (Bucket bucket : ((StringTerms) sr.getAggregations().get(AGG_NAME)).getBuckets()) {
					map.put(bucket.getKey(), (int) bucket.getDocCount());
				}
			} else {
				log.warn("Elastic clusters response error: {}", sr.status());
			}
		} catch (Exception e) {
			log.warn("Find latest count failed: {} {}, {}", field, query, e.getMessage());
		}
		return map;
	}

	private List<String> getUniqueFields(long fromWhen, String field, String query) {

		List<String> list = new ArrayList<String>();

		QueryBuilder qb = QueryBuilders.queryStringQuery(query);
		FilterBuilder fb = FilterBuilders.rangeFilter("@timestamp").from(fromWhen);

		SearchRequestBuilder sb = prepareSearch();
		sb.setSearchType(SearchType.COUNT);
		sb.setQuery(QueryBuilders.filteredQuery(qb, fb));
		sb.addAggregation(new TermsBuilder(AGG_NAME).field(field));
		try {
			SearchResponse sr = sb.execute().actionGet();
			if (RestStatus.OK.equals(sr.status())) {
				for (Bucket bucket : ((StringTerms) sr.getAggregations().get(AGG_NAME)).getBuckets()) {
					list.add(bucket.getKey());
				}
			} else {
				log.warn("Elastic clusters response error: {}", sr.status());
			}
		} catch (Exception e) {
			log.warn("Get unique field [{}] failed, {}", field, e.getMessage());
		}
		return list;
	}

	@Override
	public List<String> getLastWeekProducers(String topic) {
		long _7daysAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
		String query = String.format("datas.topic:%s AND eventType:Message.Received", topic);
		return getUniqueFields(_7daysAgo, "datas.producerIp", query);
	}

	@Override
	public List<String> getLastWeekConsumers(String topic, String consumer) {
		long _7daysAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
		String q = String.format("datas.topic:%s AND datas.groupId:%s AND eventType:Message.Delivered", topic, consumer);
		return getUniqueFields(_7daysAgo, "datas.consumerIp", q);
	}

	@Override
	public Map<String, Integer> getBrokerReceived() {
		return getLastMinuteCount("datas.brokerIp", "eventType:Message.Received", -1);
	}

	@Override
	public Map<String, Integer> getBrokerTopicReceived(String broker, int size) {
		String query = "eventType:Message.Received AND datas.brokerIp:" + broker;
		return getLastMinuteCount("datas.topic", query, size);
	}

	@Override
	public Map<String, Integer> getBrokerDelivered() {
		return getLastMinuteCount("datas.brokerIp", "eventType:Message.Delivered", -1);
	}

	@Override
	public Map<String, Integer> getBrokerTopicDelivered(String broker, int size) {
		String query = "eventType:Message.Delivered AND datas.brokerIp:" + broker;
		return getLastMinuteCount("datas.topic", query, size);
	}

	private void initClient() {
		TransportClient client = new TransportClient(ImmutableSettings.settingsBuilder()
		      .put("cluster.name", m_config.getElasticClusterName()).build());
		for (Pair<String, Integer> pair : m_config.getElasticClusterNodes()) {
			client.addTransportAddress(new InetSocketTransportAddress(pair.getKey(), pair.getValue()));
		}
		m_eClient = client;
	}

	@Override
	public void initialize() throws InitializationException {
		initClient();
	}
}
