package com.ctrip.hermes.collector.pipe;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.pipeline.annotation.Pipeline;
import com.ctrip.hermes.collector.pipeline.annotation.ProcessOn;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.ConsumeFlowState;
import com.ctrip.hermes.collector.state.ProduceFlowState;
import com.ctrip.hermes.collector.state.States;
import com.ctrip.hermes.collector.utils.JsonNodeUtils;

@Component
@Pipeline(group = "flow_pipeline_aggregation", order = 1)
@ProcessOn({ RecordType.TOPIC_FLOW_DAILY, RecordType.TOPIC_FLOW_MONTHLY })
public class TopicFlowDailyPipe extends RecordPipe {

	@Override
	public void doProcess(PipeContext context, Record<?> record) throws Exception {
		System.out.println(record.getData());
		States states = new States();
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
					ProduceFlowState state = new ProduceFlowState(String.format("%s-%s-%s", tNode.get("key").asLong(),
							pNode.get("key").asInt(), ipNode.get("key").asText()));
					state.setTopicId(id);
					state.setPartitionId(pNode.get("key").asInt());
					state.setIp(ipNode.get("key").asText());
					state.setCount(ipNode.get("doc_count").asLong());
					state.setTimestamp(record.getTimestamp());
					state.setIndex("topic-flow-aggregation");
					if (RecordType.TOPIC_FLOW_DAILY.equals(record.getType())) {
						state.setType("daily");
					} else if (RecordType.TOPIC_FLOW_MONTHLY.equals(record.getType())) {
						state.setType("monthly");
					}
					states.update(state);
				}
			}
		}

		// Iterate to get topic/consumer/partition/ip statistics.
		topics = (ArrayNode) JsonNodeUtils.findNode(data, "aggregations.consumes.group.buckets");
		tIter = topics.iterator();
		while (tIter.hasNext()) {
			JsonNode tNode = tIter.next();
			ArrayNode consumers = (ArrayNode) JsonNodeUtils.findNode(tNode, "group.buckets");
			Iterator<JsonNode> cIter = consumers.iterator();
			while (cIter.hasNext()) {
				JsonNode cNode = cIter.next();
				ArrayNode partitions = (ArrayNode) JsonNodeUtils.findNode(tNode, "group.buckets");
				Iterator<JsonNode> pIter = partitions.iterator();
				while (pIter.hasNext()) {
					JsonNode pNode = pIter.next();
					ArrayNode ips = (ArrayNode) JsonNodeUtils.findNode(pNode, "group.buckets");
					Iterator<JsonNode> ipIter = ips.iterator();
					while (ipIter.hasNext()) {
						JsonNode ipNode = ipIter.next();
						long id = tNode.get("key").asLong();
						ConsumeFlowState state = new ConsumeFlowState(String.format("%s-%s-%s-%s", tNode.get("key")
								.asLong(), cNode.get("key").asLong(), pNode.get("key").asInt(), ipNode.get("key")
								.asText()));
						state.setTopicId(id);
						state.setConsumerId(cNode.get("key").asLong());
						state.setPartitionId(pNode.get("key").asInt());
						state.setIp(ipNode.get("key").asText());
						state.setCount(ipNode.get("doc_count").asLong());
						state.setTimestamp(record.getTimestamp());
						state.setIndex("topic-flow-aggregation");
						if (RecordType.TOPIC_FLOW_DAILY.equals(record.getType())) {
							state.setType("daily");
						} else if (RecordType.TOPIC_FLOW_MONTHLY.equals(record.getType())) {
							state.setType("monthly");
						}
						states.update(state);
					}
				}
			}
		}
		context.setState(states);
		System.out.println("---------------------------" + states.size() + "------------------------");
	}
}
