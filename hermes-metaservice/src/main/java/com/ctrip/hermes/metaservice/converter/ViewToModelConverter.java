package com.ctrip.hermes.metaservice.converter;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import com.alibaba.fastjson.JSON;

public class ViewToModelConverter {

	public static com.ctrip.hermes.metaservice.model.Codec convert(com.ctrip.hermes.metaservice.view.CodecView view) {
		com.ctrip.hermes.metaservice.model.Codec model = new com.ctrip.hermes.metaservice.model.Codec();
		BeanUtils.copyProperties(view, model, getNullPropertyNames(view));
		model.setProperties(JSON.toJSONString(view.getProperties()));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.ConsumerGroup convert(
	      com.ctrip.hermes.metaservice.view.ConsumerView view) {
		com.ctrip.hermes.metaservice.model.ConsumerGroup model = new com.ctrip.hermes.metaservice.model.ConsumerGroup();
		BeanUtils.copyProperties(view, model, getNullPropertyNames(view));
		return model;
	}

	public static com.ctrip.hermes.metaservice.model.Topic convert(com.ctrip.hermes.metaservice.view.TopicView view) {
		com.ctrip.hermes.metaservice.model.Topic model = new com.ctrip.hermes.metaservice.model.Topic();
		BeanUtils.copyProperties(view, model, getNullPropertyNames(view));
		model.setProperties(JSON.toJSONString(view.getProperties()));
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
