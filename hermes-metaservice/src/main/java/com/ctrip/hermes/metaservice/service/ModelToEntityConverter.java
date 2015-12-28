package com.ctrip.hermes.metaservice.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.Property;

public class ModelToEntityConverter {

	private static void addProperties(String text, List<Property> targetList) {
		List<Property> properties = JSON.parseObject(text, new TypeReference<List<Property>>() {
		});

		for (Property entry : properties) {
			targetList.add(entry);
		}
	}

	private static void addProperties(String text, Map<String, Property> targetMap) {
		Map<String, Property> properties = JSON.parseObject(text, new TypeReference<Map<String, Property>>() {
		});

		for (Map.Entry<String, Property> entry : properties.entrySet()) {
			targetMap.put(entry.getKey(), entry.getValue());
		}
	}

	public static com.ctrip.hermes.meta.entity.App convert(com.ctrip.hermes.metaservice.model.App model) {
		com.ctrip.hermes.meta.entity.App entity = new com.ctrip.hermes.meta.entity.App();
		BeanUtils.copyProperties(model, entity);
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Codec convert(com.ctrip.hermes.metaservice.model.Codec model) {
		com.ctrip.hermes.meta.entity.Codec entity = new com.ctrip.hermes.meta.entity.Codec();
		BeanUtils.copyProperties(model, entity);
		addProperties(model.getProperties(), entity.getProperties());
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.ConsumerGroup convert(
	      com.ctrip.hermes.metaservice.model.ConsumerGroup model) {
		com.ctrip.hermes.meta.entity.ConsumerGroup entity = new com.ctrip.hermes.meta.entity.ConsumerGroup();
		BeanUtils.copyProperties(model, entity);
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Datasource convert(com.ctrip.hermes.metaservice.model.Datasource model) {
		com.ctrip.hermes.meta.entity.Datasource entity = new com.ctrip.hermes.meta.entity.Datasource();
		BeanUtils.copyProperties(model, entity);
		addProperties(model.getProperties(), entity.getProperties());
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Endpoint convert(com.ctrip.hermes.metaservice.model.Endpoint model) {
		com.ctrip.hermes.meta.entity.Endpoint entity = new com.ctrip.hermes.meta.entity.Endpoint();
		BeanUtils.copyProperties(model, entity);
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Meta convert(com.ctrip.hermes.metaservice.model.Meta model) {
		com.ctrip.hermes.meta.entity.Meta entity = new com.ctrip.hermes.meta.entity.Meta();
		entity = JSON.parseObject(model.getValue(), com.ctrip.hermes.meta.entity.Meta.class);
		BeanUtils.copyProperties(model, entity);
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Partition convert(com.ctrip.hermes.metaservice.model.Partition model) {
		com.ctrip.hermes.meta.entity.Partition entity = new com.ctrip.hermes.meta.entity.Partition();
		BeanUtils.copyProperties(model, entity);
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Producer convert(com.ctrip.hermes.metaservice.model.Producer model) {
		com.ctrip.hermes.meta.entity.Producer entity = new com.ctrip.hermes.meta.entity.Producer();
		BeanUtils.copyProperties(model, entity);
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Server convert(com.ctrip.hermes.metaservice.model.Server model) {
		com.ctrip.hermes.meta.entity.Server entity = new com.ctrip.hermes.meta.entity.Server();
		BeanUtils.copyProperties(model, entity);
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Storage convert(com.ctrip.hermes.metaservice.model.Storage model) {
		com.ctrip.hermes.meta.entity.Storage entity = new com.ctrip.hermes.meta.entity.Storage();
		BeanUtils.copyProperties(model, entity);
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Topic convert(com.ctrip.hermes.metaservice.model.Topic model) {
		com.ctrip.hermes.meta.entity.Topic entity = new com.ctrip.hermes.meta.entity.Topic();
		BeanUtils.copyProperties(model, entity);
		addProperties(model.getProperties(), entity.getProperties());
		return entity;
	}
}
