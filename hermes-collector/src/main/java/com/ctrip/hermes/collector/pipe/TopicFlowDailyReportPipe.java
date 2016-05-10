package com.ctrip.hermes.collector.pipe;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.pipeline.annotation.Pipeline;
import com.ctrip.hermes.collector.pipeline.annotation.ProcessOn;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.TopicFlowDailyReportState;
import com.ctrip.hermes.collector.state.TopicFlowDailyReportState.FlowDetail;
import com.ctrip.hermes.collector.utils.JsonNodeUtils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.model.Topic;
import com.ctrip.hermes.metaservice.model.TopicDao;
import com.ctrip.hermes.metaservice.model.TopicEntity;

@Component
@Pipeline(group = "flow_pipeline_aggregation_report", order = 1)
@ProcessOn(RecordType.TOPIC_FLOW_DAILY_REPORT)
public class TopicFlowDailyReportPipe extends RecordPipe {

	private TopicDao m_dao = PlexusComponentLocator.lookup(TopicDao.class);

	private String getBu(String topic) {
		return topic.split("[.]", 2)[0];
	}

	@Override
	public void doProcess(PipeContext context, Record<?> record) throws Exception {
		List<Topic> topicList = m_dao.list(TopicEntity.READSET_FULL);
		Map<Long, String> topicIdNameMap = new HashMap<>();
		for (Topic topic : topicList) {
			topicIdNameMap.put(topic.getId(), topic.getName());
		}
		System.out.println(record.getData());
		JsonNode data = (JsonNode) record.getData();

		TopicFlowDailyReportState state = new TopicFlowDailyReportState("DailyReport");
		state.setTimestamp(record.getTimestamp());
		state.setIndex(record.getType().getName());
		state.getTotal().setActiveTopicCount(JsonNodeUtils.findNode(data, "aggregations.topic.buckets").size());
		state.getTotal().setTotalProduce(JsonNodeUtils.findNode(data, "aggregations.produces.total").asLong());
		state.getTotal().setTotalConsume(JsonNodeUtils.findNode(data, "aggregations.consumes.total").asLong());

		// Iterate to get topic produce statistics.
		ArrayNode topics = (ArrayNode) JsonNodeUtils.findNode(data, "aggregations.produces.topic.buckets");
		state.getTotal().setHasProducedTopicCount(topics.size());
		Iterator<JsonNode> tIter = topics.iterator();
		while (tIter.hasNext()) {
			JsonNode tNode = tIter.next();
			long id = tNode.get("key").asLong();
			String topicName = topicIdNameMap.get(id);
			long count = tNode.get("sum").get("value").asLong();
			String bu = getBu(topicName);

			state.getTotal().addToTop5Produce(id, topicName, count);
			state.addToBuProduce(bu, count);
			FlowDetail detail;
			if ((detail = state.getBuDetail(bu)) != null) {
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
			long count = tNode.get("sum").get("value").asLong();
			String bu = getBu(topicName);

			state.getTotal().addToTop5Consume(id, topicName, count);
			state.addToBuConsume(bu, count);
			FlowDetail detail;
			if ((detail = state.getBuDetail(bu)) != null) {
				detail.addToTotalConsume(count);
				detail.addToTop5Consume(id, topicName, count);
				detail.addToActiveTopics(id);
				detail.hasConsumedTopicCountPlus1();
			}

		}

		for (FlowDetail detail : state.listBuDetails()) {
			detail.setActiveTopicCount(detail.getActiveTopics().size());
		}

		context.setState(state);
	}
}
