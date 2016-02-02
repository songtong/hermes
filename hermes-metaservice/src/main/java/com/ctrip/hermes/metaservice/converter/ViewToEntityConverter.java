package com.ctrip.hermes.metaservice.converter;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public class ViewToEntityConverter {

	public static com.ctrip.hermes.meta.entity.Codec convert(com.ctrip.hermes.metaservice.view.CodecView view) {
		com.ctrip.hermes.meta.entity.Codec entity = new com.ctrip.hermes.meta.entity.Codec();
		BeanUtils.copyProperties(view, entity, getNullPropertyNames(view));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.ConsumerGroup convert(com.ctrip.hermes.metaservice.view.ConsumerView view) {
		com.ctrip.hermes.meta.entity.ConsumerGroup entity = new com.ctrip.hermes.meta.entity.ConsumerGroup();
		BeanUtils.copyProperties(view, entity, getNullPropertyNames(view));
		return entity;
	}

	public static com.ctrip.hermes.meta.entity.Topic convert(com.ctrip.hermes.metaservice.view.TopicView view) {
		com.ctrip.hermes.meta.entity.Topic entity = new com.ctrip.hermes.meta.entity.Topic();
		BeanUtils.copyProperties(view, entity, getNullPropertyNames(view));
		return entity;
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
