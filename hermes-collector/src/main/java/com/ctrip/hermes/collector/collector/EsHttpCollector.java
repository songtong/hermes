package com.ctrip.hermes.collector.collector;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.record.TimeMomentRecord;
import com.ctrip.hermes.collector.record.TimeWindowRecord;
import com.ctrip.hermes.collector.utils.TypeConverter;

/**
 * @author tenglinxiao
 *
 */

@Component
public class EsHttpCollector extends HttpCollector {
	private static final Logger LOGGER = LoggerFactory.getLogger(EsHttpCollector.class);
	private static ObjectMapper m_mapper = new ObjectMapper();
	
	static {
		m_mapper.configure(Feature.AUTO_CLOSE_SOURCE, true);
	}
	
	@SuppressWarnings("unchecked")
	public Record<JsonNode> handleResponse(CollectorContext context, InputStream input) {
		EsHttpCollectorContext ctx = (EsHttpCollectorContext)context;
		try {
			Record<JsonNode> record = context.getType().newRecord();
			if (record instanceof TimeWindowRecord) {
				TimeWindowRecord<JsonNode> timeWindowRecord = (TimeWindowRecord<JsonNode>)record;
				timeWindowRecord.setStartDate(TypeConverter.toDate(ctx.getFrom()));		
				timeWindowRecord.setEndDate(TypeConverter.toDate(ctx.getTo()));
			} else {
				TimeMomentRecord<JsonNode> timeMomentRecord = (TimeMomentRecord<JsonNode>)record;
				timeMomentRecord.setMoment(TypeConverter.toDate(ctx.getFrom()));
			}
			
			// Always json data.
			JsonNode data = m_mapper.readTree(input);
			record.setData(data);
			if (retry(context, record)) {
				LOGGER.error("Retry due to failed request with ES context {} and received data {}", ctx, data);
				record = collect(context);
			}
			
			if (!ctx.getStatus().is2xxSuccessful()) {
				LOGGER.error("Final failed request with context {} and recieved data {}", ctx, data);
				return null;
			}
			context.setSucceed(true);
			return record;
		} catch (Exception e) {
			LOGGER.error("Failed to handle response for es http request: ", e);
			return null;
		} finally {
			LOGGER.info("Issued request on es with context {}", ctx);
		}
	}
	
	public static class EsHttpCollectorContext extends HttpCollectorContext {
		private long m_from;
		private long m_to;
		private String m_query;

		private EsHttpCollectorContext(HttpDatasource datasource, RecordType type) {
			super(datasource, type);
			setHeaders(new Header[]{new BasicHeader("Content-Type", "application/json")});
			setMethod(HttpMethod.POST);
		}
		
		public long getFrom() {
			return m_from;
		}

		public void setFrom(long from) {
			m_from = from;
		}

		public long getTo() {
			return m_to;
		}

		public void setTo(long to) {
			m_to = to;
		}

		public String getQuery() {
			return m_query;
		}

		public void setQuery(String query) {
			m_query = query;
		}
	}
	
	public static class EsHttpCollectorContextBuilder {
		public static final String BIZ_INDEX_NAME = "hermes-biz-%s";
		public static final String TOPIC_FLOW_INDEX_NAME = "topic-flow-%s";
		public static final String TOPIC_FLOW_AGGREGATION_INDEX_NAME = "topic-flow-aggregation-%s";
		public static final SimpleDateFormat DAILY_DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd");
		public static final SimpleDateFormat MONTHLY_DATE_FORMAT = new SimpleDateFormat("yyyy.MM");
		public static final String SEARCH_REQUEST_URL = "%s/_search";
		private EsHttpCollectorContext m_context;
		private SearchSourceBuilder m_sourceBuilder = new SearchSourceBuilder();
		
		private EsHttpCollectorContextBuilder(EsHttpCollectorContext context) {
			this.m_context = context;
		}
		
		public static EsHttpCollectorContextBuilder newContextBuilder(EsDatasource datasource, RecordType type) {
			EsHttpCollectorContext context = new EsHttpCollectorContext(datasource, type);
			EsHttpCollectorContextBuilder builder = new EsHttpCollectorContextBuilder(context);
			return builder;
		}
		
		public EsHttpCollectorContextBuilder indices(String... indices) {
			if (indices != null && indices.length > 0) {
				 m_context.setUrl(String.format(SEARCH_REQUEST_URL, StringUtils.join(indices, ",")));
			}
			
			return this;
		}
		
		public EsHttpCollectorContextBuilder url(String url) {
			if (url != null) {
				m_context.setUrl(url);
			}
			
			return this;
		}
		
		public EsHttpCollectorContextBuilder aggregationBuilders(AbstractAggregationBuilder... aggregationBuilders) {
			for (AbstractAggregationBuilder aggregation : aggregationBuilders) {
				m_sourceBuilder.aggregation(aggregation);
			}
			
			return this;
		}
		
		public EsHttpCollectorContextBuilder timeRange(long from, long to) {
			m_context.setFrom(from);
			m_context.setTo(to);
			return this;
		}
		
		public EsHttpCollectorContextBuilder queryBuilder(QueryBuilder queryBuilder) {
			m_sourceBuilder.query(queryBuilder);
			return this;
		}
		
		public EsHttpCollectorContextBuilder sortBuilder(SortBuilder sortBuilder) {
			m_sourceBuilder.sort(sortBuilder);
			return this;
		} 
		
		public EsHttpCollectorContextBuilder querySize(int size) {
			//m_context.addParameter("size", String.valueOf(size));
			m_sourceBuilder.size(size);
			return this;
		}
		
		public EsHttpCollectorContextBuilder onlyReturnAggregations() {
			//m_context.addParameter("search_type", "count");
			return querySize(0);
		}
		
		public EsHttpCollectorContext build() {
			ObjectNode request = m_mapper.createObjectNode();
			
			// Set must-have access-token
			request.put("access_token", ((EsDatasource)m_context.getDatasource()).getToken());
			request.put("request_body", m_sourceBuilder.toString());
			
			m_context.setQuery(request.toString());
			System.out.println(request.toString());
						
			try {
				m_context.setData(new StringEntity(m_context.getQuery()));
			} catch (UnsupportedEncodingException e) {
				LOGGER.error("Failed to set context data for es http request: ", e);
			}
			
			return m_context;
		}
	}
	
}

