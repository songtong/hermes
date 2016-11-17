package com.ctrip.hermes.admin.core.converter;

import org.springframework.beans.BeanUtils;

public class EntityToViewConverter {

	public static com.ctrip.hermes.admin.core.view.ConsumerGroupView convert(
	      com.ctrip.hermes.meta.entity.ConsumerGroup entity) {
		com.ctrip.hermes.admin.core.view.ConsumerGroupView view = new com.ctrip.hermes.admin.core.view.ConsumerGroupView();
		BeanUtils.copyProperties(entity, view, ConverterUtils.getNullPropertyNames(entity));
		return view;
	}

	public static com.ctrip.hermes.admin.core.view.TopicView convert(com.ctrip.hermes.meta.entity.Topic entity) {
		com.ctrip.hermes.admin.core.view.TopicView view = new com.ctrip.hermes.admin.core.view.TopicView();
		BeanUtils.copyProperties(entity, view, ConverterUtils.getNullPropertyNames(entity));
		return view;
	}

}
