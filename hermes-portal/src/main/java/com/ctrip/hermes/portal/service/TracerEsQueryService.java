package com.ctrip.hermes.portal.service;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.portal.config.PortalConfig;

@Named
public class TracerEsQueryService {

	@Inject
	private PortalConfig m_config;

	private static final Logger m_logger = LoggerFactory.getLogger(TracerEsQueryService.class);

	private static final int ES_QUERY_TIMEOUT_IN_MILLIS = 30000;

	public static final String BIZ_INDEX_NAME = "hermes-biz-%s";

	public static final SimpleDateFormat DAILY_DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd");

	private String m_token;

	private static final String m_tokenFile = "/opt/data/hermes/hermes-es.token";

	private void resolveDependency() {
		if (m_token == null) {
			BufferedReader reader = null;
			try {
				StringBuilder builder = new StringBuilder();
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(m_tokenFile)));
				String line = null;
				while ((line = reader.readLine()) != null) {
					builder.append(line.trim());
				}
				reader.close();
				m_token = builder.toString();
			} catch (Exception e) {
				m_logger.error("Failed to load es token file: {}", m_tokenFile, e);
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e) {
						// ignore.
					}
				}
			}
		}
	}

	private String getToken() {
		if (m_token == null) {
			resolveDependency();
		}
		return m_token;
	}

	public JsonNode searchBizByRefKey(long topic, String refKey, Date date) {
		SearchSourceBuilder m_sourceBuilder = new SearchSourceBuilder();
		m_sourceBuilder
		      .query(
		            QueryBuilders
		                  .boolQuery()
		                  .must(QueryBuilders.rangeQuery("@timestamp").from(date.getTime())
		                        .to(date.getTime() + TimeUnit.DAYS.toMillis(1)))
		                  .must(QueryBuilders.termQuery("topic", topic)).must(QueryBuilders.termQuery("refKey", refKey)))
		      .from(0).size(100).sort("eventTime", SortOrder.DESC).sort("eventType", SortOrder.ASC);
		String index = String.format("%s,%s", String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime())),
		      String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime() - TimeUnit.DAYS.toMillis(1))));
		return requestElasticSearch(index, date, m_sourceBuilder.toString());

	}

	public JsonNode searchBizByMsgId(long topic, int partition, long msgId, Date date) {
		// suppose message will be consumed within 2 days;
		SearchSourceBuilder m_sourceBuilder = new SearchSourceBuilder();
		m_sourceBuilder
		      .query(
		            QueryBuilders
		                  .boolQuery()
		                  .must(QueryBuilders.rangeQuery("@timestamp").from(date.getTime())
		                        .to(date.getTime() + TimeUnit.DAYS.toMillis(2)))
		                  .must(QueryBuilders.termQuery("topic", topic)).must(QueryBuilders.termQuery("msgId", msgId))
		                  .must(QueryBuilders.termQuery("partition", partition))).from(0).size(100)
		      .sort("eventTime", SortOrder.ASC);
		String index = null;
		if ((new Date().getTime() - date.getTime()) < (TimeUnit.DAYS.toMillis(1) + TimeUnit.MINUTES.toMillis(5))) {
			index = String.format("%s,%s", String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime())),
			      String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime() - TimeUnit.DAYS.toMillis(1))));
		} else {
			index = String.format("%s,%s,%s", String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime())),
			      String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime() - TimeUnit.DAYS.toMillis(1))),
			      String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime() + TimeUnit.DAYS.toMillis(1))));
		}
		return requestElasticSearch(index, date, m_sourceBuilder.toString());
	}

	public JsonNode searchBizByResendId(long topic, int partition, List<Long> resendIds, Date date) {
		// suppose message will be consumed within 2 days;
		// Result includes events which "isResend == false", U should remove them manually.
		SearchSourceBuilder m_sourceBuilder = new SearchSourceBuilder();
		m_sourceBuilder
		      .query(
		            QueryBuilders
		                  .boolQuery()
		                  .must(QueryBuilders.rangeQuery("@timestamp").from(date.getTime())
		                        .to(date.getTime() + TimeUnit.DAYS.toMillis(2)))
		                  .must(QueryBuilders.termQuery("topic", topic))
		                  .must(QueryBuilders.termsQuery("msgId", resendIds))
		                  .must(QueryBuilders.termQuery("partition", partition))
		                  .must(QueryBuilders.termQuery("eventType", "Message.Acked"))).from(0).size(100)
		      .sort("eventTime", SortOrder.ASC);
		String index = null;
		if ((new Date().getTime() - date.getTime()) < (TimeUnit.DAYS.toMillis(1) + TimeUnit.MINUTES.toMillis(5))) {
			index = String.format("%s,%s", String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime())),
			      String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime() - TimeUnit.DAYS.toMillis(1))));
		} else {
			index = String.format("%s,%s,%s", String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime())),
			      String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime() - TimeUnit.DAYS.toMillis(1))),
			      String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(date.getTime() + TimeUnit.DAYS.toMillis(1))));
		}
		return requestElasticSearch(index, date, m_sourceBuilder.toString());
	}

	private JsonNode requestElasticSearch(String index, Date date, String query) {
		String esQueryUrl = String.format("%s/%s/_search", m_config.getEsQueryUrl(), index);
		// String esQueryUrl = String.format("%s/%s/_search", "http://osg.ops.ctripcorp.com/api/10900", index);
		System.out.println(esQueryUrl);

		Map<String, Object> payload = new HashMap<>();
		payload.put("access_token", getToken());
		payload.put("request_body", query);
		InputStream is = null;
		OutputStream os = null;

		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(esQueryUrl).openConnection();
			conn.setConnectTimeout(ES_QUERY_TIMEOUT_IN_MILLIS);
			conn.setReadTimeout(ES_QUERY_TIMEOUT_IN_MILLIS);
			conn.setRequestMethod("POST");
			conn.addRequestProperty("content-type", "application/json");
			conn.setDoOutput(true);
			conn.connect();
			os = conn.getOutputStream();
			os.write(JSON.toJSONBytes(payload));
			System.out.println(payload.values());

			int statusCode = conn.getResponseCode();
			if (statusCode == 200) {
				is = conn.getInputStream();
				ObjectMapper om = new ObjectMapper();
				return om.readTree(is);
				// return IO.INSTANCE.readFrom(is, Charsets.UTF_8.name());
			} else {
				m_logger.warn("Query Es Failed! ResponseStatusCode:{}, Payload:{} !", statusCode, query);
				throw new RuntimeException(String.format("Query Es Failed! ResponseStatusCode:%s, Payload:%s!", statusCode,
				      query));
			}
		} catch (Exception e) {
			m_logger.warn("Query Es with payload:{} failed!", query, e);
			throw new RuntimeException(String.format("Query Es Failed! Payload:%s.", query), e);
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
}
