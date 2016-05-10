package com.ctrip.hermes.collector.collector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

public class JsonTest {
	@Test
	public void test() throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readTree(this.getClass().getResourceAsStream("BizLogCollectorJobTest.json"));
		org.codehaus.jackson.map.ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
		FileOutputStream output = new FileOutputStream(new File("test.json"));
		writer.writeValue(output, node);
	}
}
