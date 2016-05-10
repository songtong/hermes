package com.ctrip.hermes.collector.collector;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.CatHttpCollector.CatHttpContext;
import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContext;
import com.ctrip.hermes.collector.collector.MysqlDbCollector.MysqlDbContext;

/**
 * @author tenglinxiao
 *
 */

@Component
public class CollectorRegistry implements ApplicationContextAware {
	private ApplicationContext m_applicationContext;
	private Map<Class<?>, Collector> m_collectors = new HashMap<Class<?>, Collector>();
	
	@PostConstruct
	protected void init() {
		Map<String, Collector> collectors = m_applicationContext.getBeansOfType(Collector.class);
		for (Collector collector : collectors.values()) {
			m_collectors.put(collector.getClass(), collector);
		}
	}
	
	public Collector find(CollectorContext context) {
		if (context instanceof EsHttpCollectorContext) {
			return m_collectors.get(EsHttpCollector.class);
		} else if (context instanceof CatHttpContext) {
			return m_collectors.get(CatHttpCollector.class);
		} else if (context instanceof MysqlDbContext) {
			return m_collectors.get(MysqlDbCollector.class);
		}
		return null;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.m_applicationContext = applicationContext;
	}
}
