package com.ctrip.hermes.metaservice.converter;

import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;

public class ModelToEntityConverter {

	public static com.ctrip.hermes.meta.entity.App convert(com.ctrip.hermes.metaservice.model.App model) {
		com.ctrip.hermes.meta.entity.App entity = new com.ctrip.hermes.meta.entity.App();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Codec convert(com.ctrip.hermes.metaservice.model.Codec model) {
		com.ctrip.hermes.meta.entity.Codec entity = new com.ctrip.hermes.meta.entity.Codec();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		ConverterUtils.addProperties(model.getProperties(), entity.getProperties());
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.ConsumerGroup convert(
	      com.ctrip.hermes.metaservice.model.ConsumerGroup model) {
		com.ctrip.hermes.meta.entity.ConsumerGroup entity = new com.ctrip.hermes.meta.entity.ConsumerGroup();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Datasource convert(com.ctrip.hermes.metaservice.model.Datasource model) {
		com.ctrip.hermes.meta.entity.Datasource entity = new com.ctrip.hermes.meta.entity.Datasource();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		ConverterUtils.addProperties(model.getProperties(), entity.getProperties());
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Endpoint convert(com.ctrip.hermes.metaservice.model.Endpoint model) {
		com.ctrip.hermes.meta.entity.Endpoint entity = new com.ctrip.hermes.meta.entity.Endpoint();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Meta convert(com.ctrip.hermes.metaservice.model.Meta model) {
		com.ctrip.hermes.meta.entity.Meta entity = new com.ctrip.hermes.meta.entity.Meta();
		entity = JSON.parseObject(model.getValue(), com.ctrip.hermes.meta.entity.Meta.class);
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Partition convert(com.ctrip.hermes.metaservice.model.Partition model) {
		com.ctrip.hermes.meta.entity.Partition entity = new com.ctrip.hermes.meta.entity.Partition();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Producer convert(com.ctrip.hermes.metaservice.model.Producer model) {
		com.ctrip.hermes.meta.entity.Producer entity = new com.ctrip.hermes.meta.entity.Producer();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Server convert(com.ctrip.hermes.metaservice.model.Server model) {
		com.ctrip.hermes.meta.entity.Server entity = new com.ctrip.hermes.meta.entity.Server();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Storage convert(com.ctrip.hermes.metaservice.model.Storage model) {
		com.ctrip.hermes.meta.entity.Storage entity = new com.ctrip.hermes.meta.entity.Storage();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Topic convert(com.ctrip.hermes.metaservice.model.Topic model) {
		com.ctrip.hermes.meta.entity.Topic entity = new com.ctrip.hermes.meta.entity.Topic();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		ConverterUtils.addProperties(model.getProperties(), entity.getProperties());
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Idc convert(com.ctrip.hermes.metaservice.model.Idc model) {
		com.ctrip.hermes.meta.entity.Idc entity = new com.ctrip.hermes.meta.entity.Idc();
		BeanUtils.copyProperties(model, entity, ConverterUtils.getNullPropertyNames(model));
		if (model.getName() != null) {
			entity.setId(model.getName());
		}
		return entity;
	}

}
