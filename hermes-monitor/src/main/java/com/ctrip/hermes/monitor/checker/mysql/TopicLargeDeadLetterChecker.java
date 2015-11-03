package com.ctrip.hermes.monitor.checker.mysql;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.queue.DeadLetterDao;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.mysql.task.DeadLetterCheckerTask;

@Component(value = TopicLargeDeadLetterChecker.ID)
public class TopicLargeDeadLetterChecker extends DBBasedChecker implements InitializingBean {
	public static final String ID = "TopicLargeDeadLetterChecker";

	private static final int DEADLETTER_CHECKER_TIMEOUT_MINUTE = 3;

	private DeadLetterDao m_dao = PlexusComponentLocator.lookup(DeadLetterDao.class);

	private Map<Topic, Integer> m_limits = new HashMap<Topic, Integer>();

	@Override
	public String name() {
		return ID;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Meta meta = fetchMeta();
		for (String item : m_config.getDeadLetterCheckerIncludeTopics().split(",")) {
			String[] parts = item.split(":");
			if (parts.length < 2) {
				throw new IllegalArgumentException("Wrong dead letter checker config: " + item);
			}
			String pattern = parts[0].trim();
			Integer limit = Integer.valueOf(parts[1].trim());
			if (limit > 0) {
				for (Entry<String, Topic> entry : findTopics(pattern, meta)) {
					m_limits.put(entry.getValue(), limit);
				}
			}
		}

		for (String item : m_config.getDeadLetterCheckerExcludeTopics().split(",")) {
			if (!StringUtils.isBlank(item)) {
				for (Entry<String, Topic> entry : findTopics(item.trim(), meta)) {
					m_limits.remove(entry.getValue());
				}
			}
		}
	}

	private List<Entry<String, Topic>> findTopics(final String pattern, Meta meta) {
		return findMatched(meta.getTopics().entrySet(), new Matcher<Entry<String, Topic>>() {
			@Override
			public boolean match(Entry<String, Topic> obj) {
				return Pattern.matches(pattern, obj.getKey());
			}
		});
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		final Date to = new Date(toDate.getTime() - TimeUnit.MINUTES.toMillis(1));
		final Date from = new Date(to.getTime() - TimeUnit.MINUTES.toMillis(minutesBefore));

		final CheckerResult result = new CheckerResult();

		ExecutorService es = Executors.newFixedThreadPool(DB_CHECKER_THREAD_COUNT);
		try {
			List<Map<Topic, Integer>> splited = splitMap(m_limits, DB_CHECKER_THREAD_COUNT);
			final CountDownLatch latch = new CountDownLatch(splited.size());
			for (final Map<Topic, Integer> map : splited) {
				es.execute(new DeadLetterCheckerTask(map, m_dao, from, to, result, latch));
			}
			if (latch.await(DEADLETTER_CHECKER_TIMEOUT_MINUTE, TimeUnit.MINUTES)) {
				result.setRunSuccess(true);
			} else {
				result.setRunSuccess(false);
				result.setErrorMessage("Query dead letter db timeout, check result is not completely.");
			}
		} catch (Exception e) {
			result.setErrorMessage("Query dead letter db failed.");
			result.setException(e);
			result.setRunSuccess(false);
		} finally {
			es.shutdownNow();
		}
		return result;
	}

}
