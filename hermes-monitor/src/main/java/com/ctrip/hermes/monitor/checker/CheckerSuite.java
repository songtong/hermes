package com.ctrip.hermes.monitor.checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component
public class CheckerSuite {

	private static final Logger log = LoggerFactory.getLogger(CheckerSuite.class);

	private List<Checker> m_checkers = new ArrayList<>();

	@Autowired
	private ApplicationContext m_ctx;

	@PostConstruct
	public void afterPropertiesSet() throws Exception {
		Map<String, Checker> checkerMap = m_ctx.getBeansOfType(Checker.class);
		if (!checkerMap.isEmpty()) {
			for (Map.Entry<String, Checker> entry : checkerMap.entrySet()) {
				if (entry.getKey().startsWith("mock") || entry.getKey().startsWith("Mock")) {
					continue;
				}
				m_checkers.add(entry.getValue());
			}
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

}
