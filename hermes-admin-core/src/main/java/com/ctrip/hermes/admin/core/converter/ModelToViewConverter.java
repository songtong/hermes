package com.ctrip.hermes.admin.core.converter;

import org.springframework.beans.BeanUtils;

public class ModelToViewConverter {

	public static com.ctrip.hermes.admin.core.view.ConsumerGroupView convert(
	      com.ctrip.hermes.admin.core.model.ConsumerGroup model) {
		com.ctrip.hermes.admin.core.view.ConsumerGroupView view = new com.ctrip.hermes.admin.core.view.ConsumerGroupView();
		BeanUtils.copyProperties(model, view, ConverterUtils.getNullPropertyNames(model));
		return view;
	}

	public static com.ctrip.hermes.admin.core.view.TopicView convert(com.ctrip.hermes.admin.core.model.Topic model) {
		com.ctrip.hermes.admin.core.view.TopicView view = new com.ctrip.hermes.admin.core.view.TopicView();
		BeanUtils.copyProperties(model, view, ConverterUtils.getNullPropertyNames(model));
		ConverterUtils.addProperties(model.getProperties(), view.getProperties());
		return view;
	}

}
