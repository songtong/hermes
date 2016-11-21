package com.ctrip.hermes.admin.core.converter;

import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;

public class ViewToModelConverter {

	public static com.ctrip.hermes.admin.core.model.ConsumerGroup convert(
	      com.ctrip.hermes.admin.core.view.ConsumerGroupView view) {
		com.ctrip.hermes.admin.core.model.ConsumerGroup model = new com.ctrip.hermes.admin.core.model.ConsumerGroup();
		BeanUtils.copyProperties(view, model, ConverterUtils.getNullPropertyNames(view));
		return model;
	}

	public static com.ctrip.hermes.admin.core.model.Topic convert(com.ctrip.hermes.admin.core.view.TopicView view) {
		com.ctrip.hermes.admin.core.model.Topic model = new com.ctrip.hermes.admin.core.model.Topic();
		BeanUtils.copyProperties(view, model, ConverterUtils.getNullPropertyNames(view));
		model.setProperties(JSON.toJSONString(view.getProperties()));
		return model;
	}

}
