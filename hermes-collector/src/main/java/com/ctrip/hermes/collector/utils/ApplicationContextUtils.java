package com.ctrip.hermes.collector.utils;

import org.springframework.context.ApplicationContext;

public class ApplicationContextUtils {
	private static ApplicationContext applicationContext;
	
	public static void setApplicationContext(ApplicationContext applicationContext) {
		ApplicationContextUtils.applicationContext = applicationContext;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T getBean(Class<?> clz) {
		return (T)applicationContext.getBean(clz);
	}
}
