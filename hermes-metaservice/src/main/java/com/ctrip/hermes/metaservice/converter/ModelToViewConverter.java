package com.ctrip.hermes.metaservice.converter;

import org.springframework.beans.BeanUtils;

public class ModelToViewConverter {

	public static com.ctrip.hermes.metaservice.view.ConsumerGroupView convert(
	      com.ctrip.hermes.metaservice.model.ConsumerGroup model) {
		com.ctrip.hermes.metaservice.view.ConsumerGroupView view = new com.ctrip.hermes.metaservice.view.ConsumerGroupView();
		BeanUtils.copyProperties(model, view, ConverterUtils.getNullPropertyNames(model));
		return view;
	}

	public static com.ctrip.hermes.metaservice.view.TopicView convert(com.ctrip.hermes.metaservice.model.Topic model) {
		com.ctrip.hermes.metaservice.view.TopicView view = new com.ctrip.hermes.metaservice.view.TopicView();
		BeanUtils.copyProperties(model, view, ConverterUtils.getNullPropertyNames(model));
		ConverterUtils.addProperties(model.getProperties(), view.getProperties());
		return view;
	}

}
