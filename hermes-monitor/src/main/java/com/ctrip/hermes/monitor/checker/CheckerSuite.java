package com.ctrip.hermes.monitor.checker;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.model.MonitorEventDao;
import com.ctrip.hermes.metaservice.monitor.event.CheckerExceptionEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.monitor.config.MonitorConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component
public class CheckerSuite {

	private static final Logger log = LoggerFactory.getLogger(CheckerSuite.class);

	private List<Checker> m_checkers = new ArrayList<>();

	private ExecutorService m_executor = Executors.newFixedThreadPool(5,
	      HermesThreadFactory.create("monitorCheckerExecutor", true));

	private MonitorEventDao m_monitorEventDao = PlexusComponentLocator.lookup(MonitorEventDao.class);

	@Autowired
	private MonitorConfig m_config;

	@Autowired
	private ApplicationContext m_ctx;

	@PostConstruct
	public void afterPropertiesSet() throws Exception {
		Map<String, Checker> checkerMap = m_ctx.getBeansOfType(Checker.class);
		if (!checkerMap.isEmpty()) {
			for (Map.Entry<String, Checker> entry : checkerMap.entrySet()) {
				if (entry.getKey().toLowerCase().startsWith("mock")) {
					continue;
				}
				Checker checker = entry.getValue();
				m_checkers.add(checker);
				log.info("Found and registered checker {} of type {}.", checker.name(), checker.getClass().getName());
			}
		} else {
			log.warn("No checker found for checker suite.");
		}
	}

	@Scheduled(cron = "0 */5 * * * *")
	public void runSuite() {
		printStartInfo(m_checkers);

		Map<String, CheckerResult> results = null;
		Date toDate = new Date();
		try {
			results = runCheckers(m_checkers, toDate, 5);

			saveMonitorEvents(results, toDate);
		} catch (Exception e) {
			log.error("Exception occurred while runing checker suite. ", e);
		}

		printEndInfo(results);
	}

	private void saveMonitorEvents(Map<String, CheckerResult> results, Date toDate) {
		if (results != null) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			for (Map.Entry<String, CheckerResult> entry : results.entrySet()) {
				String checkerName = entry.getKey();
				CheckerResult result = entry.getValue();

				if (result.isRunSuccess()) {
					if (!result.getMonitorEvents().isEmpty()) {
						for (MonitorEvent event : result.getMonitorEvents()) {
							try {
								event.setShouldNotify(m_config.isMonitorCheckerNotifyEnable());
								m_monitorEventDao.insert(event.toDBEntity());
							} catch (DalException e) {
								log.error("Saven monitor event failed.", e);
							}
						}
					}
				} else {
					CheckerExceptionEvent event = new CheckerExceptionEvent();
					event.setCheckerName(checkerName);
					event.setDate(sdf.format(toDate));
					event.setDetail(String.format("[%s]Checker %s throw exception.\n %s", sdf.format(toDate), checkerName,
					      formatExceptionDetail(result.getErrorMessage(), result.getException())));
					try {
						m_monitorEventDao.insert(event.toDBEntity());
					} catch (DalException e) {
						log.error("Saven monitor event failed.", e);
					}
				}
			}
		}
	}

	private String formatExceptionDetail(String errorMessage, Exception exception) {
		StringBuilder sb = new StringBuilder();
		if (errorMessage != null) {
			sb.append(errorMessage).append("\n");
		}

		if (exception != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			exception.printStackTrace(new PrintStream(baos));
			sb.append(baos.toString());
		}

		return sb.toString();
	}

	private void printEndInfo(Map<String, CheckerResult> results) {
		log.info("-----------------------------------------------------------------");
		if (results == null) {
			log.info("Checker suite run failed!!!");
		} else {
			for (Map.Entry<String, CheckerResult> entry : results.entrySet()) {
				String checkerName = entry.getKey();
				CheckerResult result = entry.getValue();
				if (result.isRunSuccess()) {
					log.info("Checker {} success, generate {} monitor events.", checkerName, result.getMonitorEvents()
					      .size());
				} else {
					log.info("Checker {} failed(errorMessage={})!!!", checkerName, result.getErrorMessage());
				}
			}
		}
		log.info("-----------------------------------------------------------------");
	}

	private Map<String, CheckerResult> runCheckers(final List<Checker> checkers, final Date toDate,
	      final int minutesBefore) throws InterruptedException {
		Map<String, CheckerResult> results = new ConcurrentSkipListMap<>();

		CountDownLatch doneLatch = new CountDownLatch(checkers.size());

		for (Checker checker : checkers) {
			m_executor.submit(new CheckerRunningTask(checker, toDate, minutesBefore, results, doneLatch));
		}

		doneLatch.await();
		return results;
	}

	private void printStartInfo(List<Checker> checkers) {
		log.info("-----------------------------------------------------------------");
		log.info("Checker suite start...");
		log.info("Checker list: ");
		List<String> checkerNames = new ArrayList<>();
		for (Checker checker : checkers) {
			checkerNames.add(checker.name());
		}
		// sort by name
		Collections.sort(checkerNames);

		for (String checkerName : checkerNames) {
			log.info("Checker : {}", checkerName);
		}
		log.info("-----------------------------------------------------------------");
	}

	private static class CheckerRunningTask implements Runnable {

		private Checker checker;

		private Date toDate;

		private int minutesBefore;

		private Map<String, CheckerResult> results;

		private CountDownLatch doneLatch;

		public CheckerRunningTask(Checker checker, Date toDate, int minutesBefore, Map<String, CheckerResult> results,
		      CountDownLatch doneLatch) {
			super();
			this.checker = checker;
			this.toDate = toDate;
			this.minutesBefore = minutesBefore;
			this.results = results;
			this.doneLatch = doneLatch;
		}

		@Override
		public void run() {
			CheckerResult result = null;
			try {
				result = checker.check(toDate, minutesBefore);
			} catch (Exception e) {
				result = new CheckerResult();
				result.setRunSuccess(false);
				result.setErrorMessage("Uncaught exception thrown");
				result.setException(e);
			}

			if (result != null) {
				results.put(checker.name(), result);
			}

			doneLatch.countDown();
		}

	}
}
