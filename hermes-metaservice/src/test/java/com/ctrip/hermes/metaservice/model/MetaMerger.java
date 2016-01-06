package com.ctrip.hermes.metaservice.model;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.Property;

public class MetaMerger {

	@Test
	public void testReflection() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		// App
		com.ctrip.hermes.metaservice.model.App appModel = new com.ctrip.hermes.metaservice.model.App();
		appModel.setId(100);
		com.ctrip.hermes.meta.entity.App appEntity = new com.ctrip.hermes.meta.entity.App();
		BeanUtils.copyProperties(appModel, appEntity);
		System.out.println(appEntity);

		// Codec
		com.ctrip.hermes.metaservice.model.Codec codecModel = new com.ctrip.hermes.metaservice.model.Codec();
		codecModel.setType("avro");

		Map<String, Property> properties = new HashMap<String, Property>();
		Property prop1 = new Property("schema.server.url");
		prop1.setValue("http://localhost:8081");
		properties.put(prop1.getName(), prop1);

		String text = JSON.toJSONString(properties);

		com.ctrip.hermes.meta.entity.Codec codecEntity = new com.ctrip.hermes.meta.entity.Codec();
		BeanUtils.copyProperties(codecModel, codecEntity);
		addProperties(text, codecEntity.getProperties());
		System.out.println(codecEntity);

		// ConsumerGroup
		com.ctrip.hermes.metaservice.model.ConsumerGroup consumerModel = new com.ctrip.hermes.metaservice.model.ConsumerGroup();
		consumerModel.setName("consumer group");
		consumerModel.setOwner("fx");
		consumerModel.setOrderedConsume(true);
		consumerModel.setAckTimeoutSeconds(5);
		consumerModel.setDataChangeLastTime(new Date(System.currentTimeMillis() - 640000));
		consumerModel.setTopicId(101);
		consumerModel.setRetryPolicy("1:[3,6,9,15]");

		com.ctrip.hermes.meta.entity.ConsumerGroup consumerEntity = new com.ctrip.hermes.meta.entity.ConsumerGroup();
		BeanUtils.copyProperties(consumerModel, consumerEntity);
		System.out.println(consumerEntity);
		
	}

	public void addProperties(String text, Map<String, Property> targetMap) {
		Map<String, Property> properties = JSON.parseObject(text, new TypeReference<Map<String, Property>>() {
		});

		for (Map.Entry<String, Property> entry : properties.entrySet()) {
			targetMap.put(entry.getKey(), entry.getValue());
		}
	}
}
