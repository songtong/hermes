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
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.queue.MessagePriorityDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessageDao;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.exception.CompositeException;
import com.ctrip.hermes.monitor.checker.mysql.task.ConsumeBacklogCheckerTask;

@Component(value = ConsumeLargeBacklogChecker.ID)
public class ConsumeLargeBacklogChecker extends DBBasedChecker {
	public static final String ID = "ConsumeLargeBacklogChecker";

	private static final int CONSUME_BACKLOG_CHECKER_TIMEOUT_MINUTE = 5;

	private MessagePriorityDao m_msgDao = PlexusComponentLocator.lookup(MessagePriorityDao.class);

	private OffsetMessageDao m_offsetDao = PlexusComponentLocator.lookup(OffsetMessageDao.class);

	@Override
	public String name() {
		return ID;
	}

	protected void setMessagePriorityDao(MessagePriorityDao dao) {
		m_msgDao = dao;
	}

	protected void setOffsetMessageDao(OffsetMessageDao dao) {
		m_offsetDao = dao;
	}

	protected Map<Pair<Topic, ConsumerGroup>, Long> parseLimits(Meta meta, String includeString, String excludeString) {
		if (meta == null || StringUtils.isBlank(includeString) || StringUtils.isBlank(excludeString)) {
			return null;
		}
		Map<Pair<Topic, ConsumerGroup>, Long> limits = new HashMap<>();
		Map<String, Map<String, Integer>> includes = JSON.parseObject(includeString,
		      new TypeReference<Map<String, Map<String, Integer>>>() {
		      });
		Map<String, Integer> all = includes.get(".*");
		if (all != null) {
			for (Entry<String, Integer> entry : all.entrySet()) {
				limits.putAll(parseIncludes(meta, ".*", entry.getKey(), entry.getValue()));
			}
		}
		for (Entry<String, Map<String, Integer>> item : includes.entrySet()) {
			if (!item.getKey().equals(".*")) {
				for (Entry<String, Integer> entry : item.getValue().entrySet()) {
					limits.putAll(parseIncludes(meta, item.getKey(), entry.getKey(), entry.getValue()));
				}
			}
		}

		Map<String, List<String>> excludes = JSON.parseObject(excludeString,
		      new TypeReference<Map<String, List<String>>>() {
		      });
		for (Entry<String, List<String>> item : excludes.entrySet()) {
			for (String consumer : item.getValue()) {
				removeExcludes(limits, meta, item.getKey(), consumer);
			}
		}

		return limits;
	}

	private Map<Pair<Topic, ConsumerGroup>, Long> parseIncludes( //
	      Meta meta, final String topicPattern, final String groupPattern, long limit) {
		Map<Pair<Topic, ConsumerGroup>, Long> limits = new HashMap<Pair<Topic, ConsumerGroup>, Long>();
		List<Entry<String, Topic>> includeTopics = findMatched(meta.getTopics().entrySet(),
		      new Matcher<Entry<String, Topic>>() {
			      @Override
			      public boolean match(Entry<String, Topic> obj) {
				      return Pattern.matches(topicPattern.trim(), obj.getKey());
			      }
		      });
		for (Entry<String, Topic> entry : includeTopics) {
			Topic topic = entry.getValue();
			if (Storage.MYSQL.equals(topic.getStorageType())) {
				List<ConsumerGroup> includeGroups = findMatched(topic.getConsumerGroups(), new Matcher<ConsumerGroup>() {
					@Override
					public boolean match(ConsumerGroup obj) {
						return Pattern.matches(groupPattern, obj.getName());
					}
				});
				for (ConsumerGroup group : includeGroups) {
					limits.put(new Pair<Topic, ConsumerGroup>(topic, group), limit);
				}
			}
		}
		return limits;
	}

	private void removeExcludes(Map<Pair<Topic, ConsumerGroup>, Long> limits, //
	      Meta meta, final String topicPattern, final String groupPattern) {
		List<Entry<String, Topic>> excludeTopics = findMatched(meta.getTopics().entrySet(),
		      new Matcher<Entry<String, Topic>>() {
			      @Override
			      public boolean match(Entry<String, Topic> obj) {
				      return Pattern.matches(topicPattern.trim(), obj.getKey());
			      }
		      });
		for (Entry<String, Topic> entry : excludeTopics) {
			Topic topic = entry.getValue();
			List<ConsumerGroup> excludeGroups = findMatched(topic.getConsumerGroups(), new Matcher<ConsumerGroup>() {
				@Override
				public boolean match(ConsumerGroup obj) {
					return Pattern.matches(groupPattern, obj.getName());
				}
			});
			for (ConsumerGroup group : excludeGroups) {
				limits.remove(new Pair<Topic, ConsumerGroup>(topic, group));
			}
		}
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		final CheckerResult result = new CheckerResult();
		ExecutorService es = Executors.newFixedThreadPool(DB_CHECKER_THREAD_COUNT);
		try {
			Map<Pair<Topic, ConsumerGroup>, Long> limits = parseLimits(fetchMeta(), //
			      m_config.getConsumeBacklogCheckerIncludeTopics(), m_config.getConsumeBacklogCheckerExcludeTopics());
			ConcurrentSet<Exception> exceptions = new ConcurrentSet<Exception>();
			List<Map<Pair<Topic, ConsumerGroup>, Long>> splited = splitMap(limits, DB_CHECKER_THREAD_COUNT);
			final CountDownLatch latch = new CountDownLatch(splited.size());
			for (final Map<Pair<Topic, ConsumerGroup>, Long> batchLimits : splited) {
				es.execute(new ConsumeBacklogCheckerTask(batchLimits, m_msgDao, m_offsetDao, result, latch, exceptions));
			}
			if (latch.await(CONSUME_BACKLOG_CHECKER_TIMEOUT_MINUTE, TimeUnit.MINUTES)) {
				result.setRunSuccess(true);
			} else {
				result.setRunSuccess(false);
				result.setErrorMessage("Query consume backlog db timeout, check result is not completely.");
			}
			if (exceptions.size() > 0) {
				result.setRunSuccess(false);
				result.setErrorMessage("Consumer large backlog checker task has exceptions!");
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
}
