package com.ctrip.hermes.collector.strategy;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.datasource.Datasource;
import com.ctrip.hermes.collector.datasource.Datasource.DatasourceType;

@Component
public class StrategyRegistry implements ApplicationContextAware{
	private Map<DatasourceType, Strategy> strategies = new HashMap<DatasourceType, Strategy>();
	
	public Strategy findStrategy(Datasource datasource) {
		return null;
	}


	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		Map<String, Strategy> strategies = applicationContext.getBeansOfType(Strategy.class);
	}

}
