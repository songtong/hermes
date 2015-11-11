package com.ctrip.hermes.monitor.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ApplicationContextUtil implements ApplicationContextAware {
	private static ApplicationContext m_context;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		m_context = applicationContext;
	}

	public static <T> T getBean(Class<? extends T> clazz) {
		if (m_context == null) {
			throw new RuntimeException("Application context is not inited.");
		}
		return m_context.getBean(clazz);
	}
}
