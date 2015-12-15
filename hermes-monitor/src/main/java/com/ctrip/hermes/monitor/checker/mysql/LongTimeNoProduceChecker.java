package com.ctrip.hermes.monitor.checker.mysql;

import io.netty.util.internal.ConcurrentSet;

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

import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.queue.MessagePriorityDao;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.exception.CompositeException;
import com.ctrip.hermes.monitor.checker.mysql.task.LongTimeNoProduceCheckerTask;
import com.ctrip.hermes.monitor.utils.MonitorUtils;
import com.ctrip.hermes.monitor.utils.MonitorUtils.Matcher;

@Component(value = LongTimeNoProduceChecker.ID)
public class LongTimeNoProduceChecker extends DBBasedChecker {

	public static final String ID = "LongTimeNoProduceChecker";

	private static final int LONG_TIME_NO_PRODUCE_CHECKER_TIMEOUT_MINUTE = 5;

	private MessagePriorityDao m_msgDao = PlexusComponentLocator.lookup(MessagePriorityDao.class);

	@Override
	public String name() {
		return ID;
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		Meta meta = fetchMeta();
		final CheckerResult result = new CheckerResult();
		ExecutorService es = Executors.newFixedThreadPool(DB_CHECKER_THREAD_COUNT);

		try {
			Map<Topic, Integer> limits = parseLimits(meta, //
			      m_config.getLongTimeNoProduceCheckerIncludeTopics(), m_config.getLongTimeNoProduceCheckerExcludeTopics());
			ConcurrentSet<Exception> exceptions = new ConcurrentSet<Exception>();
			final CountDownLatch latch = new CountDownLatch(limits.size());
			for (final Entry<Topic, Integer> limit : limits.entrySet()) {
				es.execute(//
				new LongTimeNoProduceCheckerTask(limit.getKey(), limit.getValue(), m_msgDao, result, latch, exceptions));
			}
			if (latch.await(LONG_TIME_NO_PRODUCE_CHECKER_TIMEOUT_MINUTE, TimeUnit.MINUTES)) {
				result.setRunSuccess(true);
			} else {
				result.setRunSuccess(false);
				result.setErrorMessage("Check long time no produce timeout, check result is not completely.");
			}
			if (exceptions.size() > 0) {
				result.setRunSuccess(false);
				result.setErrorMessage("Long time no produce checker task has exceptions!");
				result.setException(new CompositeException(exceptions));
			}
		} catch (Exception e) {
			result.setErrorMessage("Query consume backlog db failed.");
			result.setException(e);
			result.setRunSuccess(false);
		} finally {
			es.shutdownNow();
		}
		return result;
	}

	protected Map<Topic, Integer> parseLimits(Meta meta, String includeString, String excludeString) {
		if (meta == null || StringUtils.isBlank(includeString) || StringUtils.isBlank(excludeString)) {
			return null;
		}
		Map<Topic, Integer> limits = new HashMap<>();
		Map<String, Integer> includes = JSON.parseObject(includeString, new TypeReference<Map<String, Integer>>() {
		});

		if (includes.containsKey(".*")) {
			limits.putAll(parseIncludes(meta, ".*", includes.get(".*")));
		}

		for (Entry<String, Integer> item : includes.entrySet()) {
			if (!item.getKey().equals(".*")) {
				limits.putAll(parseIncludes(meta, item.getKey(), item.getValue()));
			}
		}

		List<String> excludes = JSON.parseObject(excludeString, new TypeReference<List<String>>() {
		});
		for (String exclude : excludes) {
			removeExcludes(limits, meta, exclude);
		}

		return limits;
	}

	private Map<Topic, Integer> parseIncludes(Meta meta, final String topicPattern, int limit) {
		Map<Topic, Integer> limits = new HashMap<>();
		List<Entry<String, Topic>> includeTopics = MonitorUtils.findMatched(meta.getTopics().entrySet(),
		      new Matcher<Entry<String, Topic>>() {
			      @Override
			      public boolean match(Entry<String, Topic> obj) {
				      return Pattern.matches(topicPattern.trim(), obj.getKey());
			      }
		      });
		for (Entry<String, Topic> entry : includeTopics) {
			if (Storage.MYSQL.equals(entry.getValue().getStorageType())) {
				limits.put(entry.getValue(), limit);
			}
		}
		return limits;
	}

	private void removeExcludes(Map<Topic, Integer> limits, Meta meta, final String topicPattern) {
		List<Entry<String, Topic>> excludeTopics = MonitorUtils.findMatched(meta.getTopics().entrySet(),
		      new Matcher<Entry<String, Topic>>() {
			      @Override
			      public boolean match(Entry<String, Topic> obj) {
				      return Pattern.matches(topicPattern.trim(), obj.getKey());
			      }
		      });
		for (Entry<String, Topic> entry : excludeTopics) {
			limits.remove(entry.getValue());
		}
	}
}
