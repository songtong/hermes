package com.ctrip.hermes.monitor.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

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
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Files.IO;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.core.utils.StringUtils.StringFormatter;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.metaservice.service.EndpointService;
import com.ctrip.hermes.monitor.checker.server.ServerCheckerConstans;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.dashboard.DashboardItem;
import com.ctrip.hermes.monitor.domain.MonitorItem;
import com.google.common.base.Charsets;

@Service
public class ESMonitorService {

	private static final Logger log = LoggerFactory.getLogger(ESMonitorService.class);

	private static final SimpleDateFormat INDEX_DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd");

	public static final String MONITOR_INDEX_PREFIX = "monitor-";

	private static final String QUERY_AGG_NAME = "hermes-agg";

	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");

	private static final SimpleDateFormat EVENT_TIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

	private static final int ES_QUERY_TIMEOUT_IN_MILLIS = 30000;

	private static final String DEFAULT_KAFKA_BROKER_GROUP = "kafka-default";

	static {
		JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
	}

	private TransportClient client;

	private EndpointService endpointService;

	@Autowired
	private MonitorConfig config;

	@PostConstruct
	private void postConstruct() {
		endpointService = PlexusComponentLocator.lookup(EndpointService.class);
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.getEsClusterName()).build();
		client = new TransportClient(settings);
		String[] esTransportAddress = config.getEsTransportAddress();
		for (int i = 0; i < esTransportAddress.length; i++) {
			String[] split = esTransportAddress[i].split(":");
			client.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
		}
	}

	public IndexResponse prepareIndex(DashboardItem item) throws IOException {
		IndexRequestBuilder builder = client.prepareIndex(item.getIndex(), item.getCategory());
		String source = JSON.toJSONString(item, SerializerFeature.WriteDateUseDateFormat);
		IndexResponse response = builder.setSource(source).execute().actionGet();
		return response;
	}

	public IndexResponse prepareIndex(MonitorItem item) throws IOException {
		IndexRequestBuilder builder = client.prepareIndex(MONITOR_INDEX_PREFIX + INDEX_DATE_FORMAT.format(new Date()),
		      item.getCategory(), generateId(item));
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

	private List<String> toList(String jsonString) {
		return JSON.parseObject(jsonString, new TypeReference<List<String>>() {
		});
	}

	private boolean isBrokerBelongKafka(String hostname) {
		try {
			Endpoint endpoint = endpointService.findEndpoint(hostname);
			return endpoint != null && DEFAULT_KAFKA_BROKER_GROUP.equals(endpoint.getGroup());
		} catch (DalException e) {
			log.warn("Can not find broker with hostname:{}, {}", hostname, e.getMessage());
			return false;
		}
	}

	public Map<String, Long> queryBrokerErrorCount(Date from, Date to) {
		Map<String, Long> mysql = doQueryBrokerErrorCount(from, to, toList(config.getEsIgnoreMysqlBrokerMessages()));
		Map<String, Long> kafka = doQueryBrokerErrorCount(from, to, toList(config.getEsIgnoreKafkaBrokerMessages()));

		Map<String, Long> result = new HashMap<>();
		for (Entry<String, Long> entry : mysql.entrySet()) {
			if (!isBrokerBelongKafka(entry.getKey())) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		for (Entry<String, Long> entry : kafka.entrySet()) {
			if (isBrokerBelongKafka(entry.getKey())) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

	private Map<String, Long> doQueryBrokerErrorCount(Date from, Date to, List<String> excludeMessages) {
		ESQueryContext ctx = new ESQueryContext();

		ctx.setDocumentType(ServerCheckerConstans.ES_DOC_TYPE_BROKER);
		ctx.setIndex(getHermesLogIndex(from, to));
		ctx.setFrom(from);
		ctx.setTo(to);
		ctx.setGroupSchema("hostname");
		ctx.addQuery("message", "ERROR");
		for (String msg : excludeMessages) {
			ctx.addQuery("-message", msg);
		}

		return queryCountInTimeRange(ctx);
	}

	private String getHermesLogIndex(Date from, Date to) {
		List<String> idxs = new ArrayList<>();
		Date curDate = new Date(to.getTime());
		while (curDate.after(from)) {
			idxs.add(String.format(ServerCheckerConstans.ES_HERMES_LOG_INDEX_PATTERN, INDEX_DATE_FORMAT.format(curDate)));
			curDate.setTime(curDate.getTime() - TimeUnit.DAYS.toMillis(1));
		}
		Calendar cal = Calendar.getInstance();
		cal.setTime(from);
		if (cal.get(Calendar.HOUR_OF_DAY) < 8) {
			idxs.add(String.format(ServerCheckerConstans.ES_HERMES_LOG_INDEX_PATTERN, INDEX_DATE_FORMAT.format(curDate)));
		}
		return StringUtils.join(idxs, ",");
	}

	public Map<String, Long> queryMetaserverErrorCount(Date from, Date to) {
		ESQueryContext ctx = new ESQueryContext();

		ctx.setDocumentType(ServerCheckerConstans.ES_DOC_TYPE_METASERVER);
		ctx.setIndex(getHermesLogIndex(from, to));
		ctx.setFrom(from);
		ctx.setTo(to);
		ctx.setGroupSchema("hostname");
		ctx.addQuery("message", "ERROR");
		for (String message : toList(config.getEsIgnoreKafkaBrokerMessages())) {
			ctx.addQuery("-message", message);
		}

		return queryCountInTimeRange(ctx);
	}

	private String loadElasticSearchToken() throws Exception {
		File f = new File(config.getElasticSearchTokenPath());
		for (String line : Files.readAllLines(Paths.get(f.toURI()), Charsets.UTF_8)) {
			if (!StringUtils.isBlank(line)) {
				return line.trim();
			}
		}
		throw new RuntimeException("Load elastic search token failed.");
	}

	private String joinQuerys(List<Pair<String, String>> querys) {
		return StringUtils.join(querys, " AND ", new StringFormatter<Pair<String, String>>() {
			@Override
			public String format(Pair<String, String> obj) {
				return obj.getKey() + ":'" + obj.getValue() + "'";
			}
		});
	}

	private Map<String, Long> queryCountInTimeRange(ESQueryContext ctx) {
		String query = String.format("source:%s AND %s", ctx.getDocumentType(), joinQuerys(ctx.getQuerys()));

		QueryBuilder qb = QueryBuilders.queryStringQuery(query);
		FilterBuilder fb = FilterBuilders.rangeFilter("@timestamp") //
		      .from(EVENT_TIME_FORMATTER.format(ctx.getFrom())) //
		      .to(EVENT_TIME_FORMATTER.format(ctx.getTo()));
		TermsBuilder tb = new TermsBuilder(QUERY_AGG_NAME).field(ctx.getGroupSchema());

		@SuppressWarnings("resource")
		SearchRequestBuilder sb = new TransportClient().prepareSearch(ctx.getIndex());
		sb.setTypes(ctx.getDocumentType());
		sb.setSearchType(SearchType.COUNT);
		sb.setQuery(QueryBuilders.filteredQuery(qb, fb));
		sb.addAggregation(tb);

		String esQueryCondition = sb.toString();
		try {
			Map<String, Object> payload = new HashMap<>();
			payload.put("access_token", loadElasticSearchToken());
			payload.put("request_body", esQueryCondition);
			String response = requestElasticSearch(ctx.getIndex(), payload);
			if (!StringUtils.isBlank(response)) {
				return parseCountFromElasticResponse(response);
			}
		} catch (Exception e) {
			log.error("Query count from es service failed.", e);
		}
		throw new RuntimeException("Query count from es service failed.");
	}

	private Map<String, Long> parseCountFromElasticResponse(String response) {
		Map<String, Long> m = new HashMap<>();

		JSONArray ja = JSON.parseObject(response) //
		      .getJSONObject("aggregations") //
		      .getJSONObject(QUERY_AGG_NAME) //
		      .getJSONArray("buckets");

		for (Object obj : ja) {
			JSONObject jObj = (JSONObject) obj;
			String host = jObj.getString("key");
			long count = jObj.getLongValue("doc_count");
			m.put(host, count);
		}

		return m;
	}

	private String requestElasticSearch(final String indexPattern, final Object payload) {
		String path = String.format("/api/10900/%s/_search", indexPattern);
		String url = String.format("http://%s%s", config.getElasticSearchQueryHost(), path);
		InputStream is = null;
		OutputStream os = null;

		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

			conn.setConnectTimeout(ES_QUERY_TIMEOUT_IN_MILLIS);
			conn.setReadTimeout(ES_QUERY_TIMEOUT_IN_MILLIS);
			conn.setRequestMethod("POST");
			conn.addRequestProperty("content-type", "application/json");

			if (payload != null) {
				conn.setDoOutput(true);
				conn.connect();
				os = conn.getOutputStream();
				os.write(JSON.toJSONBytes(payload));
			} else {
				conn.connect();
			}

			int statusCode = conn.getResponseCode();

			if (statusCode == 200) {
				is = conn.getInputStream();
				return IO.INSTANCE.readFrom(is, Charsets.UTF_8.name());
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Response error while posting es server error({url={}, status={}}).", url, statusCode);
				}
				return null;
			}

		} catch (Exception e) {
			log.debug("Post es server error.", e);
			return null;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (Exception e) {
					// ignore it
				}
			}

			if (os != null) {
				try {
					os.close();
				} catch (Exception e) {
					// ignore it
				}
			}
		}
	}

	public MonitorItem queryLatestMonitorItem(String category) {
		SearchResponse response = client.prepareSearch(MONITOR_INDEX_PREFIX + INDEX_DATE_FORMAT.format(new Date()))
		      .setTypes(category).setSize(1).addSort("startDate", SortOrder.DESC).execute().actionGet();
		MonitorItem latestItem = null;
		SearchHits hits = response.getHits();
		if (hits.getTotalHits() > 0) {
			String result = hits.getHits()[0].getSourceAsString();
			latestItem = JSON.parseObject(result, MonitorItem.class);
		}
		return latestItem;
	}
}
