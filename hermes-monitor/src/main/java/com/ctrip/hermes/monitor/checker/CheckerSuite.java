package com.ctrip.hermes.monitor.checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component
public class CheckerSuite implements InitializingBean, ApplicationContextAware {

	private static final Logger log = LoggerFactory.getLogger(CheckerSuite.class);

	private List<Checker> m_checkers = new ArrayList<>();

	private ApplicationContext m_ctx;

	@Override
	public void afterPropertiesSet() throws Exception {
		Map<String, Checker> checkerMap = m_ctx.getBeansOfType(Checker.class);
		if (!checkerMap.isEmpty()) {
			m_checkers.addAll(checkerMap.values());
			for (Checker checker : m_checkers) {
				log.info("Found and registered checker {} of type {}.", checker.name(), checker.getClass().getName());
			}

		} else {
			log.warn("No checker found for checker suite.");
		}
	}

	@Scheduled(cron = "0 */5 * * * *")
	public void runSuite() {

	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		m_ctx = applicationContext;
	}

}
