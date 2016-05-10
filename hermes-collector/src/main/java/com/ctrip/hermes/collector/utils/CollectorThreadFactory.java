package com.ctrip.hermes.collector.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CollectorThreadFactory implements ThreadFactory {
	private AtomicInteger m_id = new AtomicInteger(0);
	private String m_category;
	
	private CollectorThreadFactory(String category) {
		this.m_category = category;
	}

	@Override
	public Thread newThread(Runnable runnable) {
		Thread t = new Thread(runnable);
		t.setName(String.format("collector_%s_%d", m_category, m_id.incrementAndGet()));
		return t;
	}
	
	
	public static ThreadFactory newFactory(String category) {
		return new CollectorThreadFactory(category);
	}
	
	public static ThreadFactory newFactory(Class<?> clazz) {
		return newFactory(clazz.getSimpleName());
	}
}
