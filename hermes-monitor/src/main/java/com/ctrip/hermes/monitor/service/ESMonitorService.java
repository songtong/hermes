package com.ctrip.hermes.monitor.service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ctrip.hermes.monitor.checker.server.ServerCheckerConstans;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.dashboard.DashboardItem;
import com.ctrip.hermes.monitor.domain.MonitorItem;

@Service
public class ESMonitorService {

	private static final Logger log = LoggerFactory.getLogger(ESMonitorService.class);

	public static final String DEFAULT_INDEX = "monitor";

	public static final String DASHBOARD_INDEX = "dashboard";

	private static final String QUERY_AGG_NAME = "hermes-agg";

	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
	static {
		JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
	}

	private TransportClient client;

	@Autowired
	private MonitorConfig config;

	@PostConstruct
	private void postConstruct() {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.getEsClusterName()).build();
		client = new TransportClient(settings);
		String[] esTransportAddress = config.getEsTransportAddress();
		for (int i = 0; i < esTransportAddress.length; i++) {
			String[] split = esTransportAddress[i].split(":");
			client.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
		}
	}

	public IndexResponse prepareIndex(DashboardItem item) throws IOException {
		IndexRequestBuilder builder = client.prepareIndex(DASHBOARD_INDEX, item.getCategory());
		String source = JSON.toJSONString(item, SerializerFeature.WriteDateUseDateFormat);
		IndexResponse response = builder.setSource(source).execute().actionGet();
		return response;
	}

	public IndexResponse prepareIndex(MonitorItem item) throws IOException {
		IndexRequestBuilder builder = client.prepareIndex(DEFAULT_INDEX, item.getCategory(), generateId(item));
		String source = JSON.toJSONString(item, SerializerFeature.WriteDateUseDateFormat);
		IndexResponse response = builder.setSource(source).execute().actionGet();
		return response;
	}

	public GetResponse prepareGet(String index, String type, String id) {
		GetResponse response = client.prepareGet(index, type, id).execute().actionGet();
		return response;
	}

	@PreDestroy
	private void preDestory() {
		client.close();
	}

	private String generateId(MonitorItem item) {
		StringBuilder sb = new StringBuilder();
		sb.append(item.getHost()).append('_').append(item.getSource()).append('_').append(item.getGroup()).append('_')
		      .append(item.getCategory()).append('_').append(formatter.format(item.getStartDate()));
		return sb.toString();
	}

	// *********************** FOR ES QUERY *********************** //

	public Map<String, Long> queryBrokerErrorCount(long from, long to) {
		ESQueryContext ctx = new ESQueryContext();

		ctx.setDocumentType(ServerCheckerConstans.ES_DOC_TYPE_BROKER);
		ctx.setIndex(ServerCheckerConstans.ES_INDEX);
		ctx.setFrom(from);
		ctx.setTo(to);
		ctx.setGroupSchema("host.raw");
		ctx.setKeyWord("ERROR");
		ctx.setQuerySchema("message");

		return queryCountInTimeRange(ctx);
	}

	public Map<String, Long> queryMetaserverErrorCount(long from, long to) {
		ESQueryContext ctx = new ESQueryContext();

		ctx.setDocumentType(ServerCheckerConstans.ES_DOC_TYPE_METASERVER);
		ctx.setIndex(ServerCheckerConstans.ES_INDEX);
		ctx.setFrom(from);
		ctx.setTo(to);
		ctx.setGroupSchema("host.raw");
		ctx.setKeyWord("ERROR");
		ctx.setQuerySchema("message");

		return queryCountInTimeRange(ctx);
	}

	private Map<String, Long> queryCountInTimeRange(ESQueryContext ctx) {

		String query = String.format("_type:%s AND %s:%s", ctx.getDocumentType(), ctx.getQuerySchema(), ctx.getKeyWord());

		QueryBuilder qb = QueryBuilders.queryStringQuery(query);
		FilterBuilder fb = FilterBuilders.rangeFilter("@timestamp").from(ctx.getFrom()).to(ctx.getTo());
		TermsBuilder tb = new TermsBuilder(QUERY_AGG_NAME).field(ctx.getGroupSchema());

		SearchRequestBuilder sb = client.prepareSearch(ctx.getIndex());
		sb.setTypes(ctx.getDocumentType());
		sb.setSearchType(SearchType.COUNT);
		sb.setQuery(QueryBuilders.filteredQuery(qb, fb));
		sb.addAggregation(tb);

		SearchResponse sr = sb.execute().actionGet();

		Map<String, Long> map = new HashMap<String, Long>();
		if (RestStatus.OK.equals(sr.status())) {
			for (Bucket bucket : ((StringTerms) sr.getAggregations().get(QUERY_AGG_NAME)).getBuckets()) {
				map.put(bucket.getKey(), bucket.getDocCount());
			}
			return map;
		}

		log.warn("Elastic clusters response error: {}", sr.status());
		throw new RuntimeException(String.format("Elastic clusters response error: %s", sr.status()));
	}

	public MonitorItem queryLatestMonitorItem(String category) {
		SearchResponse response = client.prepareSearch(DEFAULT_INDEX).setTypes(category).setSize(1)
		      .addSort("startDate", SortOrder.DESC).execute().actionGet();
		MonitorItem latestItem = null;
		SearchHits hits = response.getHits();
		if (hits.getTotalHits() > 0) {
			String result = hits.getHits()[0].getSourceAsString();
			latestItem = JSON.parseObject(result, MonitorItem.class);
		}
		return latestItem;
	}
}
