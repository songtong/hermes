package com.ctrip.hermes.metaservice.converter;

import org.springframework.beans.BeanUtils;

public class ViewToEntityConverter {

	public static com.ctrip.hermes.meta.entity.ConsumerGroup convert(com.ctrip.hermes.metaservice.view.ConsumerGroupView view) {
		com.ctrip.hermes.meta.entity.ConsumerGroup entity = new com.ctrip.hermes.meta.entity.ConsumerGroup();
		BeanUtils.copyProperties(view, entity, ConverterUtils.getNullPropertyNames(view));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Topic convert(com.ctrip.hermes.metaservice.view.TopicView view) {
		com.ctrip.hermes.meta.entity.Topic entity = new com.ctrip.hermes.meta.entity.Topic();
		BeanUtils.copyProperties(view, entity, ConverterUtils.getNullPropertyNames(view));
		return entity;
	}

}
