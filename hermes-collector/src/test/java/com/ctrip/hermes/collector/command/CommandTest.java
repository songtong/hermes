package com.ctrip.hermes.collector.command;

import junit.framework.TestCase;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import com.ctrip.hermes.collector.exception.SerializationException.DeserializeException;
import com.ctrip.hermes.collector.exception.SerializationException.SerializeException;
import com.ctrip.hermes.collector.recordcontent.BizRecordContent;

public class CommandTest extends TestCase {
	@Test
	public void testEncode() throws SerializeException, DeserializeException {
//		BizCommandData data = new BizCommandData();
//		data.setTopicName("test");
//		data.setPartition(1);
//		System.out.println(data.serialize());
//		JsonNode json = data.serialize();
//		data = new BizCommandData();
//		data.deserialize(json);
//		System.out.println(data);
//
//		
//		TimeWindowCommand<BizCommandData> command = new TimeWindowCommand<BizCommandData>();
//		command.setData(data);
//		command.setType(CommandType.TOPIC_ERROR);
//		
//		System.out.println(command.serialize());
//		
//		json = command.serialize();
//		command.deserialize(json);
//		
//		System.out.println(command.getData());
//		System.out.println(command);
	}
}
