package com.ctrip.hermes.collector.service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.collector.collector.EsHttpCollector;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContext;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContextBuilder;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.impl.ConsumeFlowState;
import com.ctrip.hermes.collector.state.impl.ProduceFlowState;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState.ErrorSample;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState.SourceType;
import com.ctrip.hermes.collector.state.impl.TopicFlowDailyReportState;
import com.ctrip.hermes.collector.state.impl.TopicFlowDailyReportState.FlowDetail;
import com.ctrip.hermes.collector.utils.IndexUtils;
import com.ctrip.hermes.collector.utils.JsonNodeUtils;
import com.ctrip.hermes.collector.utils.JsonSerializer;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.collector.utils.Utils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Message;

@Component
public class EsHttpCollectorService {
	private static final Logger LOGGER = LoggerFactory.getLogger(EsHttpCollectorService.class);

	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	@Autowired
	private EsHttpCollector m_esCollector;
	
	public EsHttpCollectorContext createQueryContextForMetaServerError(EsDatasource datasource, long from, long to) {
		EsHttpCollectorContextBuilder contextBuilder = EsHttpCollectorContextBuilder.newContextBuilder(datasource, RecordType.METASERVER_ERROR);
		contextBuilder.timeRange(from, to);
		
		BoolQueryBuilder queryBuilder = QueryBuilders
	      .boolQuery()
	      .must(QueryBuilders.rangeQuery("@timestamp").from(from).to(to))
	      .must(QueryBuilders.termQuery("source", "metaserver"))
	      .must(QueryBuilders.termQuery("message", "error"));
		
		for (String message : toArray(m_conf.getEsIgnoreMetaserverMessages())) {
			queryBuilder.mustNot(QueryBuilders.matchPhraseQuery("message", message));
		}

		contextBuilder.queryBuilder(queryBuilder);
		
		contextBuilder.aggregationBuilders(AggregationBuilders.terms("group").field("hostname"));
		
		contextBuilder.querySize(100);
		contextBuilder.indices(IndexUtils.getLogIndex(from));

		return contextBuilder.build();
	}
	
	public EsHttpCollectorContext createQueryContextForBrokerError(EsDatasource datasource, long from, long to) {
		EsHttpCollectorContextBuilder contextBuilder = EsHttpCollectorContextBuilder.newContextBuilder(datasource, RecordType.BROKER_ERROR);
		contextBuilder.timeRange(from, to);

		contextBuilder.queryBuilder(QueryBuilders
		      .boolQuery()
		      .must(QueryBuilders.rangeQuery("@timestamp").from(from).to(to))
		      .must(QueryBuilders.termQuery("source", "broker"))
		      .must(QueryBuilders.termQuery("message", "error"))
		      .mustNot(
		            QueryBuilders
		                  .boolQuery()
		                  .must(QueryBuilders.termsQuery("hostname", m_conf.getKafkaEndPointHosts().split(",")))
		                  .must(generateOrPhraseQuery("message",
		                        toArray(m_conf.getEsIgnoreKafkaBrokerMessages())))));
		contextBuilder.aggregationBuilders(AggregationBuilders.terms("group").field("hostname"));
		contextBuilder.querySize(100);
		contextBuilder.indices(IndexUtils.getLogIndex(from));
			
		return contextBuilder.build();
	}
	
	public EsHttpCollectorContext createQueryContextForMaxDate(EsDatasource datasource, long from, long to, String url) {
		EsHttpCollectorContextBuilder builder = EsHttpCollectorContextBuilder.newContextBuilder(datasource, RecordType.MAX_DATE);
		builder.timeRange(from, to);
		builder.url(url);
	
		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("eventTime").from(from).to(to);
		builder.queryBuilder(queryBuilder);
		builder.sortBuilder(SortBuilders.fieldSort("eventTime").order(SortOrder.DESC));
		builder.querySize(1);
		return builder.build();
	}
	
	public EsHttpCollectorContext createQueryContextForTopicFlow(EsDatasource datasource, long from, long to) {
		EsHttpCollectorContextBuilder contextBuilder = EsHttpCollectorContextBuilder.newContextBuilder(datasource, RecordType.TOPIC_FLOW);
		contextBuilder.timeRange(from, to);

		// Time range query.
		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("eventTime").from(from).to(to);
		contextBuilder.queryBuilder(queryBuilder);

		// Create aggregation query for topic-producer.
//		QueryFilterBuilder queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders
//				.queryStringQuery("eventType: Message.Received"));
		TermFilterBuilder termFilterBuilder = FilterBuilders.termFilter("eventType", "Message.Received");
		FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("produces").filter(
				termFilterBuilder);
		filterAggregationBuilder.subAggregation(createAggregationBuilder(new String[]{"topic", "partition", "producerIp"}, new int[]{m_conf.getMaxTopicNumber(), m_conf.getMaxTopicPartitionNumber(), m_conf.getMaxProduceIps()}));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		// Create aggregation query for topic-consumer.
		termFilterBuilder = FilterBuilders.termFilter("eventType", "Message.Acked");

		filterAggregationBuilder = AggregationBuilders.filter("consumes").filter(termFilterBuilder);
		filterAggregationBuilder
				.subAggregation(createAggregationBuilder(new String[]{"topic", "groupId", "partition", "consumerIp"}, new int[]{m_conf.getMaxTopicNumber(), m_conf.getMaxConsumerNumber(), m_conf.getMaxTopicPartitionNumber(), m_conf.getMaxConsumerIps()}));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		contextBuilder.indices(IndexUtils.getBizIndex(from));
		return contextBuilder.onlyReturnAggregations().build();
	}

	public EsHttpCollectorContext createQueryContextForTopicFlowAggregation(EsDatasource datasource, long from, long to) {
		EsHttpCollectorContextBuilder contextBuilder = EsHttpCollectorContextBuilder.newContextBuilder(datasource, RecordType.TOPIC_FLOW_DAILY);
		contextBuilder.timeRange(from, to);
		
		// Time range query.
		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("@timestamp").from(from).to(to);
		contextBuilder.queryBuilder(queryBuilder);
		
		SumBuilder sumBuilder = AggregationBuilders.sum("sum").field("count");

		// Create aggregation query for topic-producer.
//		QueryFilterBuilder queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders
//				.queryStringQuery(String.format("%s:%s or __serialize_type__:com.ctrip.hermes.collector.state.ProduceFlowState", JsonSerializer.TYPE, ProduceFlowState.class.getName())));
		TermFilterBuilder termFilterBuilder = FilterBuilders.termFilter(JsonSerializer.TYPE, ProduceFlowState.class.getName());
		FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("produces").filter(
				termFilterBuilder);
		
		filterAggregationBuilder.subAggregation(createAggregationBuilder(sumBuilder, new String[]{"topicId", "partitionId", "ip"}, new int[]{m_conf.getMaxTopicNumber(), m_conf.getMaxTopicPartitionNumber(), m_conf.getMaxProduceIps()}));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		// Create aggregation query for topic-consumer.
//		queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders
//				.queryStringQuery(String.format("%s:%s or __serialize_type__:com.ctrip.hermes.collector.state.ConsumeFlowState", JsonSerializer.TYPE, ConsumeFlowState.class.getName())));
		termFilterBuilder = FilterBuilders.termFilter(JsonSerializer.TYPE, ConsumeFlowState.class.getName());
		filterAggregationBuilder = AggregationBuilders.filter("consumes").filter(termFilterBuilder);
		filterAggregationBuilder.subAggregation(createAggregationBuilder(sumBuilder, new String[]{"topicId", "consumerId", "partitionId", "ip"}, new int[]{m_conf.getMaxTopicNumber(), m_conf.getMaxConsumerNumber(), m_conf.getMaxTopicPartitionNumber(), m_conf.getMaxConsumerIps()}));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		contextBuilder.indices(IndexUtils.getDataQueryIndex(RecordType.TOPIC_FLOW.getCategory().getName(), from, false),
				IndexUtils.getDataQueryIndex(RecordType.TOPIC_FLOW.getCategory().getName(), to, false));
		return contextBuilder.onlyReturnAggregations().build();
	}
	
	public EsHttpCollectorContext createQueryContextForTopicFlowDailyReport(EsDatasource datasource, long from, long to) {
		EsHttpCollectorContextBuilder contextBuilder = EsHttpCollectorContextBuilder.newContextBuilder(datasource, RecordType.TOPIC_FLOW_DAILY);
		contextBuilder.timeRange(from, to);

		// Time range query.
		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("@timestamp").from(from).to(to);
		contextBuilder.queryBuilder(queryBuilder);

		// Create aggregation query for topic-producer.
//		QueryFilterBuilder queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders
//				.queryStringQuery(String.format("%s:%s", JsonSerializer.TYPE, ProduceFlowState.class.getName())));
		TermFilterBuilder termFilterBuilder = FilterBuilders.termFilter(JsonSerializer.TYPE, ProduceFlowState.class.getName());
		FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("produces").filter(
				termFilterBuilder);
		filterAggregationBuilder.subAggregation(AggregationBuilders.sum("total").field("count")).subAggregation(
				AggregationBuilders.terms("topic").field("topicId").size(m_conf.getMaxTopicNumber()).order(Order.aggregation("sum", false))
						.subAggregation(AggregationBuilders.sum("sum").field("count")));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		// Create aggregation query for topic-consume.
//		queryFilterBuilder = FilterBuilders.queryFilter(QueryBuilders
//				.queryStringQuery(String.format("%s:%s", JsonSerializer.TYPE, ConsumeFlowState.class.getName())));
		termFilterBuilder = FilterBuilders.termFilter(JsonSerializer.TYPE, ConsumeFlowState.class.getName());
		filterAggregationBuilder = AggregationBuilders.filter("consumes").filter(termFilterBuilder);
		filterAggregationBuilder.subAggregation(AggregationBuilders.sum("total").field("count")).subAggregation(
				AggregationBuilders.terms("topic").field("topicId").size(m_conf.getMaxTopicNumber()).order(Order.aggregation("sum", false))
						.subAggregation(AggregationBuilders.sum("sum").field("count")));

		contextBuilder.aggregationBuilders(filterAggregationBuilder);

		contextBuilder.aggregationBuilders(AggregationBuilders.terms("topic").field("topicId").size(m_conf.getMaxTopicNumber()));

		contextBuilder.indices(
				IndexUtils.getDataQueryIndex(RecordType.TOPIC_FLOW_DAILY.getCategory().getName(), to, true));
		
		return contextBuilder.onlyReturnAggregations().build();
	}
	
	public long getMaxTimestampFromResponse(Record<?> record) throws ParseException {
		if (isRecordValid(record)) {
			ArrayNode hits = (ArrayNode)JsonNodeUtils.findNode((JsonNode)record.getData(), "hits.hits");
			if (hits.size() > 0) {
				return TimeUtils.parseTimeFromString(JsonNodeUtils.findNode(hits.get(0), "_source.eventTime").asText());
			}
		}
		return -1;
	}
	
	public List<State> getTopicFlowStatesFromResponse(Record<?> record) {
		if (isRecordValid(record)) {
			List<State> states = new ArrayList<>();
			JsonNode data = (JsonNode) record.getData();

			// Iterate to get topic/partition/ip statistics.
			ArrayNode topics = (ArrayNode) JsonNodeUtils.findNode(data, "aggregations.produces.group.buckets");
			Iterator<JsonNode> tIter = topics.iterator();
			while (tIter.hasNext()) {
				JsonNode tNode = tIter.next();
				ArrayNode partitions = (ArrayNode) JsonNodeUtils.findNode(tNode, "group.buckets");
				Iterator<JsonNode> pIter = partitions.iterator();
				while (pIter.hasNext()) {
					JsonNode pNode = pIter.next();
					ArrayNode ips = (ArrayNode) JsonNodeUtils.findNode(pNode, "group.buckets");
					Iterator<JsonNode> ipIter = ips.iterator();
					while (ipIter.hasNext()) {
						JsonNode ipNode = ipIter.next();
						long id = tNode.get("key").asLong();
						ProduceFlowState state = new ProduceFlowState();
						state.setTopicId(id);
						state.setPartitionId(pNode.get("key").asInt());
						state.setIp(ipNode.get("key").asText());
						state.setCount(ipNode.get("doc_count").asLong());
						state.setTimestamp(record.getTimestamp());
						state.setIndex(record.getType().getName());
						states.add(state);
					}
				}
			}
			

			int produces = states.size();
			Event event = Cat.newEvent("TopicFlow", "Produces");
			event.addData("*count", produces);
			event.setStatus(Message.SUCCESS);
			event.complete();
			
			// Iterate to get topic/consumer/partition/ip statistics.
			topics = (ArrayNode) JsonNodeUtils.findNode(data, "aggregations.consumes.group.buckets");
			tIter = topics.iterator();
			while (tIter.hasNext()) {
				JsonNode tNode = tIter.next();
				ArrayNode consumers = (ArrayNode) JsonNodeUtils.findNode(tNode, "group.buckets");
				Iterator<JsonNode> cIter = consumers.iterator();
				while (cIter.hasNext()) {
					JsonNode cNode = cIter.next();
					ArrayNode partitions = (ArrayNode) JsonNodeUtils.findNode(cNode, "group.buckets");
					Iterator<JsonNode> pIter = partitions.iterator();
					while (pIter.hasNext()) {
						JsonNode pNode = pIter.next();
						ArrayNode ips = (ArrayNode) JsonNodeUtils.findNode(pNode, "group.buckets");
						Iterator<JsonNode> ipIter = ips.iterator();
						while (ipIter.hasNext()) {
							JsonNode ipNode = ipIter.next();
							long id = tNode.get("key").asLong();
							ConsumeFlowState state = new ConsumeFlowState();
							state.setTopicId(id);
							state.setConsumerId(cNode.get("key").asLong());
							state.setPartitionId(pNode.get("key").asInt());
							state.setIp(ipNode.get("key").asText());
							state.setCount(ipNode.get("doc_count").asLong());
							state.setTimestamp(record.getTimestamp());
							state.setIndex(record.getType().getName());
							states.add(state);
						}
					}
				}
			}
			
			event = Cat.newEvent("TopicFlow", "Consumes");
			event.addData("*count", states.size() - produces);
			event.setStatus(Message.SUCCESS);
			event.complete();
			return states;
		}
		
		return null;
	}
	
	public List<State> getTopicFlowAggregationStatesFromResponse(Record<?> record) {
		if (isRecordValid(record)) {
			List<State> states = new ArrayList<>();
			JsonNode data = (JsonNode) record.getData();
			
			// Iterate to get topic/partition/ip statistics.
			ArrayNode topics = (ArrayNode) JsonNodeUtils.findNode(data, "aggregations.produces.group.buckets");
			Iterator<JsonNode> tIter = topics.iterator();
			while (tIter.hasNext()) {
				JsonNode tNode = tIter.next();
				ArrayNode partitions = (ArrayNode) JsonNodeUtils.findNode(tNode, "group.buckets");
				Iterator<JsonNode> pIter = partitions.iterator();
				while (pIter.hasNext()) {
					JsonNode pNode = pIter.next();
					ArrayNode ips = (ArrayNode) JsonNodeUtils.findNode(pNode, "group.buckets");
					Iterator<JsonNode> ipIter = ips.iterator();
					while (ipIter.hasNext()) {
						JsonNode ipNode = ipIter.next();
						long id = tNode.get("key").asLong();
						ProduceFlowState state = new ProduceFlowState();
						state.setTopicId(id);
						state.setPartitionId(pNode.get("key").asInt());
						state.setIp(ipNode.get("key").asText());
						state.setCount(JsonNodeUtils.findNode(ipNode, "sum.value").asLong());
						state.setTimestamp(record.getTimestamp());
						state.setIndex(IndexUtils.getDataStoredIndex(record.getType().getCategory().getName(), true));
						states.add(state);
					}
				}
			}
			
			int produces = states.size();
			Event event = Cat.newEvent("TopicFlowAggregation", "Produces");
			event.addData("*count", produces);
			event.setStatus(Message.SUCCESS);
			event.complete();
			

			// Iterate to get topic/consumer/partition/ip statistics.
			topics = (ArrayNode) JsonNodeUtils.findNode(data, "aggregations.consumes.group.buckets");
			tIter = topics.iterator();
			while (tIter.hasNext()) {
				JsonNode tNode = tIter.next();
				ArrayNode consumers = (ArrayNode) JsonNodeUtils.findNode(tNode, "group.buckets");
				Iterator<JsonNode> cIter = consumers.iterator();
				while (cIter.hasNext()) {
					JsonNode cNode = cIter.next();
					ArrayNode partitions = (ArrayNode) JsonNodeUtils.findNode(cNode, "group.buckets");
					Iterator<JsonNode> pIter = partitions.iterator();
					while (pIter.hasNext()) {
						JsonNode pNode = pIter.next();
						ArrayNode ips = (ArrayNode) JsonNodeUtils.findNode(pNode, "group.buckets");
						Iterator<JsonNode> ipIter = ips.iterator();
						while (ipIter.hasNext()) {
							JsonNode ipNode = ipIter.next();
							long id = tNode.get("key").asLong();
							ConsumeFlowState state = new ConsumeFlowState();
							state.setTopicId(id);
							state.setConsumerId(cNode.get("key").asLong());
							state.setPartitionId(pNode.get("key").asInt());
							state.setIp(ipNode.get("key").asText());
							state.setCount(JsonNodeUtils.findNode(ipNode, "sum.value").asLong());
							state.setTimestamp(record.getTimestamp());
							state.setIndex(IndexUtils.getDataStoredIndex(record.getType().getCategory().getName(), true));
							states.add(state);
						}
					}
				}
			}
			event = Cat.newEvent("TopicFlowAggregation", "Consumes");
			event.addData("*count", states.size() - produces);
			event.setStatus(Message.SUCCESS);
			event.complete();

			return states;
		}
		return null;
	}
	
	public List<State> getDailyReportStateFromResponse(Record<?> record) throws DalException { 
		if (isRecordValid(record)) {
			List<Topic> topicList = m_topicDao.list(TopicEntity.READSET_FULL);
			Map<Long, String> topicIdNameMap = new HashMap<>();
			for (Topic topic : topicList) {
				topicIdNameMap.put(topic.getId(), topic.getName());
			}
			
			JsonNode data = (JsonNode) record.getData();
	
			List<State> states = new ArrayList<>();
			TopicFlowDailyReportState state = new TopicFlowDailyReportState("DailyReport");
			state.setTimestamp(record.getTimestamp() - TimeUnit.DAYS.toMillis(1));
			state.setIndex(record.getType().getName());
			state.getTotal().setActiveTopicCount(JsonNodeUtils.findNode(data, "aggregations.topic.buckets").size());
			state.getTotal().setTotalProduce(JsonNodeUtils.findNode(data, "aggregations.produces.total.value").asLong());
			state.getTotal().setTotalConsume(JsonNodeUtils.findNode(data, "aggregations.consumes.total.value").asLong());
	
			// Iterate to get topic produce statistics.
			ArrayNode topics = (ArrayNode) JsonNodeUtils.findNode(data, "aggregations.produces.topic.buckets");
			state.getTotal().setHasProducedTopicCount(topics.size());
			Iterator<JsonNode> tIter = topics.iterator();
			while (tIter.hasNext()) {
				JsonNode tNode = tIter.next();
				long id = tNode.get("key").asLong();
				String topicName = topicIdNameMap.get(id);
				if (topicName == null) {
					//TODO WHEN NOT TOPIC WAS FOUND, FIX IT LATTER
					continue;
				}
				long count = tNode.get("sum").get("value").asLong();
				String bu = Utils.correctBuName(Utils.getBu(topicName));
				state.getTotal().addToTop5Produce(id, topicName, count);

				if (!StringUtils.isBlank(bu)) {
					state.addToBuProduce(bu, count);
					FlowDetail detail = state.getBuFlowDetail(bu);
					detail.addToTotalProduce(count);
					detail.addToTop5Produce(id, topicName, count);
					detail.addToActiveTopics(id);
					detail.hasProducedTopicCountPlus1();
				}
			}
	
			// Iterate to get topic consume statistics.
			topics = (ArrayNode) JsonNodeUtils.findNode(data, "aggregations.consumes.topic.buckets");
			state.getTotal().setHasConsumedTopicCount(topics.size());
			tIter = topics.iterator();
			while (tIter.hasNext()) {
				JsonNode tNode = tIter.next();
				long id = tNode.get("key").asLong();
				String topicName = topicIdNameMap.get(id);
				if (topicName == null) {
					//TODO WHEN NOT TOPIC WAS FOUND, FIX IT LATTER
					continue;
				}
				long count = tNode.get("sum").get("value").asLong();
				String bu = Utils.correctBuName(Utils.getBu(topicName));
	
				state.getTotal().addToTop5Consume(id, topicName, count);
				if (!StringUtils.isBlank(bu)) {
					state.addToBuConsume(bu, count);
					FlowDetail detail = state.getBuFlowDetail(bu);
					detail.addToTotalConsume(count);
					detail.addToTop5Consume(id, topicName, count);
					detail.addToActiveTopics(id);
					detail.hasConsumedTopicCountPlus1();
				}
			}
	
			for (FlowDetail detail : state.getBuDetails().values()) {
				detail.setActiveTopicCount(detail.getActiveTopics().size());
			}
			
			states.add(state);
			return states;
		}
		return null;
	}
	
	public List<State> getBrokerErrorStateFromResponse(Record<?> record) {
		if (isRecordValid(record)) {
			List<State> states = new ArrayList<>();
			JsonNode node = (JsonNode)record.getData();
			JsonNode total = JsonNodeUtils.findNode(node, "hits.total");
			// No error state is recorded when there is no error msg found.
			if (total.getLongValue() == 0) {
				return states;
			}
			
			ServiceErrorState state = ServiceErrorState.newServiceErrorState(SourceType.BROKER);
			state.setCount(total.getLongValue());
			
			// List errors as samples.
			ArrayNode errors = (ArrayNode)JsonNodeUtils.findNode(node, "hits.hits");
			Iterator<JsonNode> eIter = errors.iterator();
			while (eIter.hasNext()) {
				JsonNode error = eIter.next().get("_source");
				ErrorSample sample = new ErrorSample();
				sample.setTimestamp(error.get("@timestamp").asLong());
				sample.setHostname(error.get("hostname").asText());
				sample.setMessage(error.get("message").asText());
				state.addErrorOnHost(sample.getHostname(), sample);
			}
			
			// List error host buckets statistics.
			errors = (ArrayNode)JsonNodeUtils.findNode(node, "aggregations.group.buckets");
			eIter = errors.iterator();
			Map<String, Long> counts = new HashMap<String, Long>();
			while (eIter.hasNext()) {
				JsonNode error = eIter.next();
				counts.put(error.get("key").asText(), error.get("doc_count").asLong());
			}
			state.setCountOnHosts(counts);
			state.setTimestamp(record.getTimestamp());
			state.setIndex(IndexUtils.getDataStoredIndex(RecordType.BROKER_ERROR.getCategory().getName(), false));
			states.add(state);
			return states;
		}
		return null;
	}
	
	public List<State> getMetaErrorStateFromResponse(Record<?> record) {
		if (isRecordValid(record)) {
			List<State> states = new ArrayList<>();
			JsonNode node = (JsonNode)record.getData();
			JsonNode total = JsonNodeUtils.findNode(node, "hits.total");
			// No error state is recorded when there is no error msg found.
			if (total.getLongValue() == 0) {
				return states;
			}
			
			ServiceErrorState state = ServiceErrorState.newServiceErrorState(SourceType.METASERVER);
			state.setCount(total.getLongValue());
			ArrayNode errors = (ArrayNode)JsonNodeUtils.findNode(node, "hits.hits");
			Iterator<JsonNode> eIter = errors.iterator();
			while (eIter.hasNext()) {
				JsonNode error = eIter.next().get("_source");
				ErrorSample sample = new ErrorSample();
				sample.setTimestamp(error.get("@timestamp").asLong());
				sample.setHostname(error.get("hostname").asText());
				sample.setMessage(error.get("message").asText());
				state.addErrorOnHost(sample.getHostname(), sample);
			}
			
			errors = (ArrayNode)JsonNodeUtils.findNode(node, "aggregations.group.buckets");
			eIter = errors.iterator();
			Map<String, Long> counts = new HashMap<String, Long>();
			while (eIter.hasNext()) {
				JsonNode error = eIter.next();
				counts.put(error.get("key").asText(), error.get("doc_count").asLong());
			}
			state.setCountOnHosts(counts);
			
			state.setTimestamp(record.getTimestamp());
			state.setIndex(IndexUtils.getDataStoredIndex(RecordType.METASERVER_ERROR.getCategory().getName(), false));
			states.add(state);
			return states;
		}
		return null;
	}
	
	public boolean isRecordValid(Record<?> record) {
		return record != null && record.getData() != null; 
	}
	
	public AggregationBuilder createAggregationBuilder(SumBuilder sumBuilder, String[] fields, int[] sizes) {
		if (fields.length != sizes.length) {
			throw new RuntimeException("Fields length must be the same as sizes length!");
		}
		
		TermsBuilder termsBuilder = null;
		TermsBuilder subTermsBuilder = null;
		for (int index = 0; index < fields.length; index++) {
			if (subTermsBuilder == null) {
				subTermsBuilder = AggregationBuilders.terms("group").size(sizes[index]);
				termsBuilder = subTermsBuilder;
			} else {
				TermsBuilder newTermsBuilder = AggregationBuilders.terms("group").size(sizes[index]);
				subTermsBuilder.subAggregation(newTermsBuilder);
				subTermsBuilder = newTermsBuilder;
			}
			subTermsBuilder.field(fields[index]);
		}
		subTermsBuilder.subAggregation(sumBuilder);
		return termsBuilder;
	}
	
	public AggregationBuilder createAggregationBuilder(String[] fields, int[] sizes) {
		if (fields.length != sizes.length) {
			throw new RuntimeException("Fields length must be the same as sizes length!");
		}
		
		TermsBuilder termsBuilder = null; 
		TermsBuilder subTermsBuilder = null;
		for (int index = 0; index < fields.length; index++) {
			if (subTermsBuilder == null) {
				subTermsBuilder = AggregationBuilders.terms("group").size(sizes[index]);
				termsBuilder = subTermsBuilder;
			} else {
				TermsBuilder newTermsBuilder = AggregationBuilders.terms("group").size(sizes[index]);
				subTermsBuilder.subAggregation(newTermsBuilder);
				subTermsBuilder = newTermsBuilder;
			}
			subTermsBuilder.field(fields[index]);
		}
		return termsBuilder;
	}
	
	public static BoolQueryBuilder generateOrPhraseQuery(String field, String[] phrases) {
		if (phrases == null || phrases.length == 0) {
			return null;
		}
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		for (String phrase : phrases) {
			boolQuery.should(QueryBuilders.matchPhraseQuery(field, phrase));
		}
		boolQuery.minimumNumberShouldMatch(1);
		return boolQuery;
	}
	
	public static String[] toArray(String jsonString) {
		return JSON.parseObject(jsonString, new TypeReference<String[]>() {
		});
	}
}
