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
import com.ctrip.hermes.admin.core.queue.MessagePriorityDao;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.DBBasedChecker;
import com.ctrip.hermes.monitor.checker.exception.CompositeException;
import com.ctrip.hermes.monitor.checker.mysql.task.LongTimeNoProduceCheckerTask;
import com.ctrip.hermes.monitor.utils.MonitorUtils;
import com.ctrip.hermes.monitor.utils.MonitorUtils.Matcher;

//@Component(value = LongTimeNoProduceChecker.ID)
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
		final CheckerResult result = new CheckerResult();
		if (m_config.isMonitorCheckerEnable()) {
			Meta meta = fetchMeta();
			ExecutorService es = Executors.newFixedThreadPool(DB_CHECKER_THREAD_COUNT);

			try {
				Map<Topic, Map<Integer, Integer>> limits = parseLimits(
				      meta, //
				      m_config.getLongTimeNoProduceCheckerIncludeTopics(),
				      m_config.getLongTimeNoProduceCheckerExcludeTopics());
				ConcurrentSet<Exception> exceptions = new ConcurrentSet<Exception>();
				final CountDownLatch latch = new CountDownLatch(limits.size());
				for (final Entry<Topic, Map<Integer, Integer>> limit : limits.entrySet()) {
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
		}
		return result;
	}

	protected Map<Topic, Map<Integer, Integer>> parseLimits(Meta meta, String includeString, String excludeString) {
		if (meta == null || StringUtils.isBlank(includeString) || StringUtils.isBlank(excludeString)) {
			return null;
		}
		Map<Topic, Map<Integer, Integer>> limits = new HashMap<>();
		Map<String, Map<Integer, Integer>> includesPatterns = JSON.parseObject(includeString,
		      new TypeReference<Map<String, Map<Integer, Integer>>>() {
		      });

		if (includesPatterns.containsKey(".*")) {
			limits.putAll(parseIncludes(meta, ".*", includesPatterns.get(".*")));
		}

		for (Entry<String, Map<Integer, Integer>> item : includesPatterns.entrySet()) {
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

	private Map<Topic, Map<Integer, Integer>> parseIncludes(//
	      Meta meta, final String topicPattern, Map<Integer, Integer> limit) {
		Map<Topic, Map<Integer, Integer>> limits = new HashMap<>();
		List<Entry<String, Topic>> includeTopics = MonitorUtils.findMatched(meta.getTopics().entrySet(),
		      new Matcher<Entry<String, Topic>>() {
			      @Override
			      public boolean match(Entry<String, Topic> obj) {
				      return Pattern.matches(topicPattern.trim(), obj.getKey());
			      }
		      });
		for (Entry<String, Topic> entry : includeTopics) {
			if (Storage.MYSQL.equals(entry.getValue().getStorageType())) {
				Map<Integer, Integer> partitionLimits = new HashMap<>();
				if (limit.containsKey(-1)) {
					for (Partition partition : entry.getValue().getPartitions()) {
						partitionLimits.put(partition.getId(), limit.get(-1));
					}
				}
				for (Entry<Integer, Integer> p2l : limit.entrySet()) {
					if (p2l.getKey() != -1) {
						partitionLimits.put(p2l.getKey(), p2l.getValue());
					}
				}
				limits.put(entry.getValue(), partitionLimits);
			}
		}
		return limits;
	}

	private void removeExcludes(Map<Topic, Map<Integer, Integer>> limits, Meta meta, final String topicPattern) {
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
