package com.ctrip.hermes.metaservice.converter;

import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;

public class ViewToModelConverter {

	public static com.ctrip.hermes.metaservice.model.ConsumerGroup convert(
	      com.ctrip.hermes.metaservice.view.ConsumerGroupView view) {
		com.ctrip.hermes.metaservice.model.ConsumerGroup model = new com.ctrip.hermes.metaservice.model.ConsumerGroup();
		BeanUtils.copyProperties(view, model, ConverterUtils.getNullPropertyNames(view));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Topic convert(com.ctrip.hermes.metaservice.view.TopicView view) {
		com.ctrip.hermes.metaservice.model.Topic model = new com.ctrip.hermes.metaservice.model.Topic();
		BeanUtils.copyProperties(view, model, ConverterUtils.getNullPropertyNames(view));
		model.setProperties(JSON.toJSONString(view.getProperties()));
		return model;
	}

}
