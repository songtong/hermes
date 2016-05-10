package com.ctrip.hermes.collector.collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.State;

public class EsQueryContextBuilder {
	private EsQueryContext m_context;
	private SearchRequestBuilder m_requestBuilder;
	// private RangeFilterBuilder m_rangeFilterBuilder;
	private static ObjectMapper mapper = new ObjectMapper();

	public static EsQueryContextBuilder newBuilder(EsQueryType type) {
		EsQueryContextBuilder contextBuilder = new EsQueryContextBuilder();
		if (type == EsQueryType.QUERY) {
			contextBuilder.newQueryContext();
		} else {
			contextBuilder.newCreateContext();
		}
		return contextBuilder;
	}

	@SuppressWarnings("resource")
	private EsQueryContextBuilder() {
		m_requestBuilder = new TransportClient().prepareSearch("");
	}

	private EsQueryContextBuilder newQueryContext() {
		m_context = new EsQueryContext();
		return this;
	}

	private EsQueryContextBuilder newCreateContext() {
		m_context = new EsQueryContext(EsQueryType.CREATE);
		m_context.setQueryType(EsQueryType.CREATE);
		m_context.setType("data");
		return this;
	}

	public EsQueryContextBuilder useDefaultBizIndex() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(m_context.getFrom() - TimeUnit.HOURS.toMillis(8));
		m_context.getIndices().add(
				String.format(EsQueryContext.BIZ_INDEX_NAME, EsQueryContext.DATE_FORMAT.format(calendar.getTime())));
		return this;
	}

	public EsQueryContextBuilder useDefaultTopicFlowIndex(Long date) {
		m_context.getIndices().add(
				String.format(EsQueryContext.TOPIC_FLOW_INDEX_NAME, EsQueryContext.DATE_FORMAT.format(date)));
		return this;
	}
	
	public EsQueryContextBuilder useDefaultTopicFlowAggregationIndex() {
		m_context.getIndices().add(EsQueryContext.TOPIC_FLOW_AGGREGATION_INDEX_NAME);
		return this;
	}

	public EsQueryContextBuilder useDefaultRecordIndex(RecordType type) {
		m_context.getIndices().add(type.getName());
		return this;
	}

	public EsQueryContextBuilder timeRange(long from, long to) {
		m_context.setFrom(from);
		m_context.setTo(to);
		return this;
	}

	public EsQueryContextBuilder data(List<State> data) {
		m_context.setData(data);
		return this;
	}

	public EsQueryContextBuilder queryBuilder(QueryBuilder queryBuilder) {
		m_requestBuilder.setQuery(queryBuilder);
		return this;
	}

	@SuppressWarnings("rawtypes")
	public EsQueryContextBuilder aggregationBuilders(AggregationBuilder... aggregationBuilders) {
		for (AggregationBuilder aggregation : aggregationBuilders) {
			m_requestBuilder.addAggregation(aggregation);
		}
		return this;
	}

	public EsQueryContext build() {
		if (EsQueryType.CREATE == m_context.getQueryType()) {
			StringBuilder queryBuilder = new StringBuilder();
			for (State state : m_context.getData()) {
				ObjectNode operation = mapper.createObjectNode();
				ObjectNode header = mapper.createObjectNode();
				header.put("_index", state.getIndex());
				header.put("_type", state.getType());
				operation.put("create", header);
				queryBuilder.append(operation.toString() + "\n");
				queryBuilder.append(state.toString() + "\n");
			}
			m_context.setQuery(queryBuilder.toString());
		} else {
			m_context.setQuery(m_requestBuilder.toString());
		}
		return m_context;
	}

	public static class EsQueryContext {
		public static final String BIZ_INDEX_NAME = "hermes-biz-%s";
		public static final String TOPIC_FLOW_INDEX_NAME = "topic-flow-%s";
		public static final String TOPIC_FLOW_AGGREGATION_INDEX_NAME = "topic-flow-aggregation";
		public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd");
		private EsQueryType m_queryType;
		private List<String> m_indices = new ArrayList<String>();
		private String m_type;
		private long m_from;
		private long m_to;
		private String m_query;
		private List<State> m_data;

		private EsQueryContext() {
			this(EsQueryType.QUERY);
		}

		private EsQueryContext(EsQueryType queryType) {
			m_queryType = queryType;
		}

		public String getType() {
			return m_type;
		}

		public void setType(String type) {
			m_type = type;
		}

		public EsQueryType getQueryType() {
			return m_queryType;
		}

		public void setQueryType(EsQueryType queryType) {
			m_queryType = queryType;
		}

		public List<String> getIndices() {
			return m_indices;
		}

		public void setIndices(List<String> indices) {
			m_indices = indices;
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

		public List<State> getData() {
			return m_data;
		}

		public void setData(List<State> data) {
			m_data = data;
		}

		public void addState(State state) {
			if (m_data == null) {
				m_data = new ArrayList<State>();
			}
			m_data.add(state);
		}

		public String getQuery() {
			return m_query;
		}

		public void setQuery(String query) {
			m_query = query;
		}

		public static EsQueryContext newQueryContext(String... indices) {
			EsQueryContext context = new EsQueryContext();
			if (indices == null || indices.length == 0) {
				context.getIndices().add(
						String.format(BIZ_INDEX_NAME, DATE_FORMAT.format(Calendar.getInstance().getTime())));
			} else {
				for (String index : indices) {
					context.getIndices().add(index);
				}
			}
			return context;
		}

		public static EsQueryContext newCreateContext() {
			EsQueryContext context = new EsQueryContext(EsQueryType.CREATE);
			return context;
		}

	}

	public static enum EsQueryType {
		CREATE, QUERY;
	}

}
