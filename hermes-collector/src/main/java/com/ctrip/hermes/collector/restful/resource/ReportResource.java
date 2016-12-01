package com.ctrip.hermes.collector.restful.resource;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.JsonNode;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.EsHttpCollector;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContextBuilder;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.impl.ConsumeFlowState;
import com.ctrip.hermes.collector.state.impl.ProduceFlowState;
import com.ctrip.hermes.collector.utils.IndexUtils;
import com.ctrip.hermes.collector.utils.JsonNodeUtils;
import com.ctrip.hermes.collector.utils.JsonSerializer;
import com.ctrip.hermes.collector.utils.Utils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

@Component
@Path("/report")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ReportResource {	
	private static final Logger LOGGER = LoggerFactory.getLogger(ReportResource.class);
	
	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	@Autowired
	private EsHttpCollector m_collector;
	
	@Autowired
	private DatasourceManager m_datasourceManager;
	
	@GET
	@Path("topic")
	public Response getTopicStatistics() {
		try {
			List<Topic> topicList = m_topicDao.list(TopicEntity.READSET_FULL);

			Map<String, TopicStatistics> result = new HashMap<>();

			for (Topic topic : topicList) {
				if (!result.containsKey(topic.getStorageType())) {
					result.put(topic.getStorageType(), new TopicStatistics(topic.getStorageType()));
				}
				
				TopicStatistics statistics = result.get(topic.getStorageType());
				String bu = Utils.getBu(topic.getName());
				statistics.increseCount();
				statistics.increseBuCount(bu);				
			}
			return Response.status(Status.OK).entity(result).build();
		} catch (DalException e) {
			LOGGER.error("Failed to get statistics on topic due to exception: ", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(String.format("Failed to get statistics on topic due to exception: %s", e.getMessage())).build();
		} 
	}
	
	
	public class TopicStatistics {
		private String m_storageType;
		private int m_count;
		private Map<String, Integer> m_buDistributions = new HashMap<>();
		
		public TopicStatistics(String storageType) {
			this.m_storageType = storageType;
		}
		public String getStorageType() {
			return m_storageType;
		}
		public void setStorageType(String storageType) {
			m_storageType = storageType;
		}
		public int getCount() {
			return m_count;
		}
		public void setCount(int count) {
			m_count = count;
		}
		public Map<String, Integer> getBuDistributions() {
			return m_buDistributions;
		}
		public void setBuDistributions(Map<String, Integer> buDistributions) {
			m_buDistributions = buDistributions;
		}
		public void increseCount() {
			this.m_count++;
		}
		public void increseBuCount(String bu) {
			String mapped = Utils.correctBuName(bu);
			if (!m_buDistributions.containsKey(mapped)) {
				m_buDistributions.put(mapped, 1);
			} else {
				m_buDistributions.put(mapped, m_buDistributions.get(mapped) + 1);
			}
		}
	}
	
	@GET
	@Path("topic-flow")
	public Response getTopicFlowReport(@QueryParam("from") @DefaultValue("0") long from, @QueryParam("to") @DefaultValue("0") long to) {
		if (from < 0 || to < 0 || from > to || to > System.currentTimeMillis()) {
			return Response.status(Status.BAD_REQUEST).entity(String.format("Invalid params for from & to: %d, %d", from, to)).build();
		} else if (from == 0 && to == 0) {
			Calendar c = Calendar.getInstance();
			to = c.getTimeInMillis();
			
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
			from = c.getTimeInMillis();
		}
		
		CollectorContext context = createContext(from, to);
		try {
			Record<?> record = m_collector.collect(context);
			if (record != null) {
				return Response.status(Status.OK).entity(parseForData(record)).build();
			}
			
			throw new RuntimeException("Request got NULL record.");
		} catch (Exception e) {
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(String.format("Failed to fetch report due to exception: %s", e.getMessage())).build();
		}
	}
	
	private Map<String, Object> parseForData(Record<?> record) {
		if (record.getData() == null) {
			return null;
		}
		
		JsonNode data = (JsonNode) record.getData();
		long produces = JsonNodeUtils.findNode(data, "aggregations.produces.total.value").asLong();
		long consumes = JsonNodeUtils.findNode(data, "aggregations.consumes.total.value").asLong();
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("produces", produces);
		result.put("consumes", consumes);
		return result;
	}
	
	private CollectorContext createContext(long from, long to) {
		EsDatasource datasource = (EsDatasource) m_datasourceManager.getDefaultDatasource(HttpDatasourceType.ES);
		EsHttpCollectorContextBuilder contextBuilder = EsHttpCollectorContextBuilder.newContextBuilder(datasource, RecordType.TOPIC_FLOW);
		contextBuilder.timeRange(from, to);

		// Time range query.
		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("@timestamp").from(from).to(to);
		contextBuilder.queryBuilder(queryBuilder);

		// Create aggregation query for topic-producer.
		FilterBuilder termFilterBuilder = FilterBuilders.termFilter(JsonSerializer.TYPE, ProduceFlowState.class.getName());
		FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("produces").filter(
				termFilterBuilder);
		filterAggregationBuilder.subAggregation(AggregationBuilders.sum("total").field("count"));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		// Create aggregation query for topic-consume.
		termFilterBuilder = FilterBuilders.termFilter(JsonSerializer.TYPE, ConsumeFlowState.class.getName());
		filterAggregationBuilder = AggregationBuilders.filter("consumes").filter(termFilterBuilder);
		filterAggregationBuilder.subAggregation(AggregationBuilders.sum("total").field("count"));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		contextBuilder.indices(
				IndexUtils.getDataQueryIndices(RecordType.TOPIC_FLOW_DAILY.getCategory().getName(), from, to, false));
		
		return contextBuilder.onlyReturnAggregations().build();
	} 
}
