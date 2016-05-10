package com.ctrip.hermes.collector.recordcontent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import com.ctrip.hermes.collector.record.RecordContent;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.States;
import com.ctrip.hermes.collector.state.TopicFlowState;
import com.ctrip.hermes.collector.utils.JsonNodeUtils;

public class TopicFlowContent extends RecordContent {
	public TopicFlowContent() {
		//setState(new States());
	}

	public void bind(JsonNode json) {

		
	}
}
