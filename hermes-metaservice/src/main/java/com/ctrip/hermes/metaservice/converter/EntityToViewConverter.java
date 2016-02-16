package com.ctrip.hermes.metaservice.converter;

import org.springframework.beans.BeanUtils;

public class EntityToViewConverter {

	public static com.ctrip.hermes.metaservice.view.ConsumerGroupView convert(
	      com.ctrip.hermes.meta.entity.ConsumerGroup entity) {
		com.ctrip.hermes.metaservice.view.ConsumerGroupView view = new com.ctrip.hermes.metaservice.view.ConsumerGroupView();
		BeanUtils.copyProperties(entity, view, ConverterUtils.getNullPropertyNames(entity));
		return view;
	}

	public static com.ctrip.hermes.metaservice.view.TopicView convert(com.ctrip.hermes.meta.entity.Topic entity) {
		com.ctrip.hermes.metaservice.view.TopicView view = new com.ctrip.hermes.metaservice.view.TopicView();
		BeanUtils.copyProperties(entity, view, ConverterUtils.getNullPropertyNames(entity));
		return view;
	}

}
