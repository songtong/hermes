package com.ctrip.hermes.collector.pipe;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.collector.pipeline.annotation.Pipeline;
import com.ctrip.hermes.collector.pipeline.annotation.ProcessOn;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.impl.TopicFlowDailyReportState;
import com.ctrip.hermes.collector.state.impl.TopicFlowDailyReportState.FlowDetail;
import com.ctrip.hermes.collector.utils.JsonNodeUtils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

@Component
@Pipeline(group = "flow_pipeline_aggregation_report", order = 1)
@ProcessOn(RecordType.TOPIC_FLOW_DAILY_REPORT)
public class TopicFlowDailyReportPipe extends RecordPipe {

	private TopicDao m_dao = PlexusComponentLocator.lookup(TopicDao.class);

	private String getBu(String topic) {
		return topic.split("[.]", 2)[0];
	}
	
	private String correctBu(String bu) {
		switch(bu) {
		case "bbz": return "basebiz";
		case "mkt": return "marketing";
		default: return bu;
		}
	}

	@Override
	public void doProcess(PipeContext context, Record<?> record) throws Exception {
		List<Topic> topicList = m_dao.list(TopicEntity.READSET_FULL);
		Map<Long, String> topicIdNameMap = new HashMap<>();
		for (Topic topic : topicList) {
			topicIdNameMap.put(topic.getId(), topic.getName());
		}
		JsonNode data = (JsonNode) record.getData();

		TopicFlowDailyReportState state = new TopicFlowDailyReportState("DailyReport");
		state.setTimestamp(record.getTimestamp() - TimeUnit.DAYS.toMillis(1));
		state.setIndex(record.getType().getName());
		state.getTotal().setActiveTopicCount(JsonNodeUtils.findNode(data, "aggregations.topic.buckets").size());
		state.getTotal().setTotalProduce(JsonNodeUtils.findNode(data, "aggregations.produces.total.value").asLong());
		state.getTotal().setTotalConsume(JsonNodeUtils.findNode(data, "aggregations.consumes.total.value").asLong());

		// TODO refactor
		List<String> bus = Arrays.asList("hotel", "bbz", "basebiz", "flight", "mkt", "marketing");

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
			String bu = getBu(topicName);

			state.getTotal().addToTop5Produce(id, topicName, count);
			state.addToBuProduce(bu, count);
			if (bus.contains(bu)) {
				FlowDetail detail = state.getBuFlowDetail(correctBu(bu));
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
			if (bus.contains(bu)) {
				FlowDetail detail = state.getBuFlowDetail(correctBu(bu));
				detail.addToTotalConsume(count);
				detail.addToTop5Consume(id, topicName, count);
				detail.addToActiveTopics(id);
				detail.hasConsumedTopicCountPlus1();
			}
		}

		for (FlowDetail detail : state.getBuDetails().values()) {
			detail.setActiveTopicCount(detail.getActiveTopics().size());
		}
		context.setState(state);
	}
}
