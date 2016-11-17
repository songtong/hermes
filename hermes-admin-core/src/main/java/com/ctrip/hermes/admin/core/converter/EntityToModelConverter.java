package com.ctrip.hermes.admin.core.converter;

import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;

public class EntityToModelConverter {

	public static com.ctrip.hermes.admin.core.model.App convert(com.ctrip.hermes.meta.entity.App entity) {
		com.ctrip.hermes.admin.core.model.App model = new com.ctrip.hermes.admin.core.model.App();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Codec convert(com.ctrip.hermes.meta.entity.Codec entity) {
		com.ctrip.hermes.admin.core.model.Codec model = new com.ctrip.hermes.admin.core.model.Codec();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		model.setProperties(JSON.toJSONString(entity.getProperties()));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.ConsumerGroup convert(
	      com.ctrip.hermes.meta.entity.ConsumerGroup entity) {
		com.ctrip.hermes.admin.core.model.ConsumerGroup model = new com.ctrip.hermes.admin.core.model.ConsumerGroup();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Datasource convert(com.ctrip.hermes.meta.entity.Datasource entity) {
		com.ctrip.hermes.admin.core.model.Datasource model = new com.ctrip.hermes.admin.core.model.Datasource();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		model.setProperties(JSON.toJSONString(entity.getProperties()));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Endpoint convert(com.ctrip.hermes.meta.entity.Endpoint entity) {
		com.ctrip.hermes.admin.core.model.Endpoint model = new com.ctrip.hermes.admin.core.model.Endpoint();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Meta convert(com.ctrip.hermes.meta.entity.Meta entity) {
		com.ctrip.hermes.metaservice.model.Meta model = new com.ctrip.hermes.metaservice.model.Meta();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		model.setValue(JSON.toJSONString(entity));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Partition convert(com.ctrip.hermes.meta.entity.Partition entity) {
		com.ctrip.hermes.admin.core.model.Partition model = new com.ctrip.hermes.admin.core.model.Partition();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Producer convert(com.ctrip.hermes.meta.entity.Producer entity) {
		com.ctrip.hermes.admin.core.model.Producer model = new com.ctrip.hermes.admin.core.model.Producer();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Server convert(com.ctrip.hermes.meta.entity.Server entity) {
		com.ctrip.hermes.admin.core.model.Server model = new com.ctrip.hermes.admin.core.model.Server();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Storage convert(com.ctrip.hermes.meta.entity.Storage entity) {
		com.ctrip.hermes.admin.core.model.Storage model = new com.ctrip.hermes.admin.core.model.Storage();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		model.setProperties(JSON.toJSONString(entity.getProperties()));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Topic convert(com.ctrip.hermes.meta.entity.Topic entity) {
		com.ctrip.hermes.admin.core.model.Topic model = new com.ctrip.hermes.admin.core.model.Topic();
		BeanUtils.copyProperties(entity, model, ConverterUtils.getNullPropertyNames(entity));
		model.setProperties(JSON.toJSONString(entity.getProperties()));
		return model;
	}

}
