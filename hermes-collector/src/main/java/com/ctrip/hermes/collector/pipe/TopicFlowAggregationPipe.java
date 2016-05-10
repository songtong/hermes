package com.ctrip.hermes.collector.pipe;

import org.codehaus.jackson.JsonNode;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.pipeline.annotation.Pipeline;
import com.ctrip.hermes.collector.pipeline.annotation.ProcessOn;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;


@Component
@Pipeline(group="flow_pipeline_aggregation", order=1)
@ProcessOn(RecordType.TOPIC_FLOW)
public class TopicFlowAggregationPipe extends RecordPipe {

	@Override
	public void doProcess(PipeContext context, Record<?> record) throws Exception {
		JsonNode data = (JsonNode)record.getData();
		data.get("hits");
	}
	
}
