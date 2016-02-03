package com.ctrip.hermes.metaservice.converter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.Property;

public class ModelToViewConverter {
	public static com.ctrip.hermes.metaservice.view.CodecView convert(com.ctrip.hermes.metaservice.model.Codec model) {
		com.ctrip.hermes.metaservice.view.CodecView view = new com.ctrip.hermes.metaservice.view.CodecView();
		BeanUtils.copyProperties(model, view, getNullPropertyNames(model));
		addProperties(model.getProperties(), view.getProperties());
		return view;
	}

	public static com.ctrip.hermes.metaservice.view.ConsumerView convert(
	      com.ctrip.hermes.metaservice.model.ConsumerGroup model) {
		com.ctrip.hermes.metaservice.view.ConsumerView view = new com.ctrip.hermes.metaservice.view.ConsumerView();
		BeanUtils.copyProperties(model, view, getNullPropertyNames(model));
		return view;
	}

	public static com.ctrip.hermes.metaservice.view.TopicView convert(com.ctrip.hermes.metaservice.model.Topic model) {
		com.ctrip.hermes.metaservice.view.TopicView view = new com.ctrip.hermes.metaservice.view.TopicView();
		BeanUtils.copyProperties(model, view, getNullPropertyNames(model));
		addProperties(model.getProperties(), view.getProperties());
		return view;
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

	private static void addProperties(String text, List<Property> targetList) {
		List<Property> properties = JSON.parseObject(text, new TypeReference<List<Property>>() {
		});

		for (Property entry : properties) {
			targetList.add(entry);
		}
	}

}
