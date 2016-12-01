package com.ctrip.hermes.collector.utils;

import org.codehaus.jackson.JsonNode;

public class JsonNodeUtils {
	public static JsonNode findNode(JsonNode data, String path) {
		String[] paths = path.split("\\.");
		JsonNode node = data;
		for (int index = 0; index < paths.length; index++) {
			if ((node = node.path(paths[index])) == null) {
				break;
			}
		}
		return node;
	}

}
