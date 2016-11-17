package com.ctrip.hermes.admin.core.converter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.Property;

public class ConverterUtils {

	static String[] getNullPropertyNames(Object source) {
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

	static void addProperties(String text, List<Property> targetList) {
		List<Property> properties = JSON.parseObject(text, new TypeReference<List<Property>>() {
		});

		for (Property entry : properties) {
			targetList.add(entry);
		}
	}

	static void addProperties(String text, Map<String, Property> targetMap) {
		Map<String, Property> properties = JSON.parseObject(text, new TypeReference<Map<String, Property>>() {
		});

		for (Map.Entry<String, Property> entry : properties.entrySet()) {
			targetMap.put(entry.getKey(), entry.getValue());
		}
	}
}
