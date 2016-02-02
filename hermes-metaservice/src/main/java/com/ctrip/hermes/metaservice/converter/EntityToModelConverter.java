package com.ctrip.hermes.metaservice.converter;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import com.alibaba.fastjson.JSON;

public class EntityToModelConverter {

	public static com.ctrip.hermes.metaservice.model.App convert(com.ctrip.hermes.meta.entity.App entity) {
		com.ctrip.hermes.metaservice.model.App model = new com.ctrip.hermes.metaservice.model.App();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Codec convert(com.ctrip.hermes.meta.entity.Codec entity) {
		com.ctrip.hermes.metaservice.model.Codec model = new com.ctrip.hermes.metaservice.model.Codec();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		model.setProperties(JSON.toJSONString(entity.getProperties()));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.ConsumerGroup convert(
	      com.ctrip.hermes.meta.entity.ConsumerGroup entity) {
		com.ctrip.hermes.metaservice.model.ConsumerGroup model = new com.ctrip.hermes.metaservice.model.ConsumerGroup();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Datasource convert(com.ctrip.hermes.meta.entity.Datasource entity) {
		com.ctrip.hermes.metaservice.model.Datasource model = new com.ctrip.hermes.metaservice.model.Datasource();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		model.setProperties(JSON.toJSONString(entity.getProperties()));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Endpoint convert(com.ctrip.hermes.meta.entity.Endpoint entity) {
		com.ctrip.hermes.metaservice.model.Endpoint model = new com.ctrip.hermes.metaservice.model.Endpoint();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Meta convert(com.ctrip.hermes.meta.entity.Meta entity) {
		com.ctrip.hermes.metaservice.model.Meta model = new com.ctrip.hermes.metaservice.model.Meta();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		model.setValue(JSON.toJSONString(entity));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Partition convert(com.ctrip.hermes.meta.entity.Partition entity) {
		com.ctrip.hermes.metaservice.model.Partition model = new com.ctrip.hermes.metaservice.model.Partition();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Producer convert(com.ctrip.hermes.meta.entity.Producer entity) {
		com.ctrip.hermes.metaservice.model.Producer model = new com.ctrip.hermes.metaservice.model.Producer();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Server convert(com.ctrip.hermes.meta.entity.Server entity) {
		com.ctrip.hermes.metaservice.model.Server model = new com.ctrip.hermes.metaservice.model.Server();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Storage convert(com.ctrip.hermes.meta.entity.Storage entity) {
		com.ctrip.hermes.metaservice.model.Storage model = new com.ctrip.hermes.metaservice.model.Storage();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		model.setProperties(JSON.toJSONString(entity.getProperties()));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Topic convert(com.ctrip.hermes.meta.entity.Topic entity) {
		com.ctrip.hermes.metaservice.model.Topic model = new com.ctrip.hermes.metaservice.model.Topic();
		BeanUtils.copyProperties(entity, model, getNullPropertyNames(entity));
		model.setProperties(JSON.toJSONString(entity.getProperties()));
		return model;
	}

	private static String[] getNullPropertyNames(Object source) {
		final BeanWrapper src = new BeanWrapperImpl(source);
		java.beans.PropertyDescriptor[] pds = src.getPropertyDescriptors();

		Set<String> emptyNames = new HashSet<String>();
		for (java.beans.PropertyDescriptor pd : pds) {
			Object srcValue = src.getPropertyValue(pd.getName());
			if (srcValue == null)
				emptyNames.add(pd.getName());
		}
		String[] result = new String[emptyNames.size()];
		return emptyNames.toArray(result);
	}
}
