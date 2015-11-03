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
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.queue.MessagePriorityDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessageDao;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.mysql.task.ConsumeBacklogCheckerTask;

@Component(value = ConsumeLargeBacklogChecker.ID)
public class ConsumeLargeBacklogChecker extends DBBasedChecker implements InitializingBean {
	public static final String ID = "ConsumeLargeBacklogChecker";

	private static final int CONSUME_BACKLOG_CHECKER_TIMEOUT_MINUTE = 5;

	private MessagePriorityDao m_msgDao = PlexusComponentLocator.lookup(MessagePriorityDao.class);

	private OffsetMessageDao m_offsetDao = PlexusComponentLocator.lookup(OffsetMessageDao.class);

	private Map<Pair<Topic, ConsumerGroup>, Long> m_limits = new HashMap<>();

	@Override
	public String name() {
		return ID;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Meta meta = fetchMeta();

		for (String item : m_config.getConsumeBacklogCheckerIncludeTopics().split(",")) {
			final String[] parts = item.split(":");
			if (parts.length < 3) {
				throw new IllegalAccessException("Wrong format of consume backlog checker include config: " + item);
			}
			addInclude(meta, parts[0], parts[1], Long.valueOf(parts[2]));
		}

		for (String item : m_config.getConsumeBacklogCheckerExcludeTopics().split(",")) {
			final String[] parts = item.split(":");
			if (parts.length < 2) {
				throw new IllegalAccessException("Wrong format of consume backlog checker exclude config: " + item);
			}
			addExclude(meta, parts[0], parts[1]);
		}
	}

	private void addInclude(Meta meta, final String topicPattern, final String groupPattern, long limit) {
		List<Entry<String, Topic>> includeTopics = findMatched(meta.getTopics().entrySet(),
		      new Matcher<Entry<String, Topic>>() {
			      @Override
			      public boolean match(Entry<String, Topic> obj) {
				      return Pattern.matches(topicPattern.trim(), obj.getKey());
			      }
		      });
		for (Entry<String, Topic> entry : includeTopics) {
			Topic topic = entry.getValue();
			List<ConsumerGroup> includeGroups = findMatched(topic.getConsumerGroups(), new Matcher<ConsumerGroup>() {
				@Override
				public boolean match(ConsumerGroup obj) {
					return Pattern.matches(groupPattern, obj.getName());
				}
			});
			for (ConsumerGroup group : includeGroups) {
				m_limits.put(new Pair<Topic, ConsumerGroup>(topic, group), limit);
			}
		}
	}

	private void addExclude(Meta meta, final String topicPattern, final String groupPattern) {
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
				m_limits.remove(new Pair<Topic, Integer>(topic, group.getId()));
			}
		}
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {

		final CheckerResult result = new CheckerResult();

		ExecutorService es = Executors.newFixedThreadPool(DB_CHECKER_THREAD_COUNT);
		try {
			List<Map<Pair<Topic, ConsumerGroup>, Long>> splited = splitMap(m_limits, DB_CHECKER_THREAD_COUNT);
			final CountDownLatch latch = new CountDownLatch(splited.size());
			for (final Map<Pair<Topic, ConsumerGroup>, Long> limits : splited) {
				es.execute(new ConsumeBacklogCheckerTask(limits, m_msgDao, m_offsetDao, result, latch));
			}
			if (latch.await(CONSUME_BACKLOG_CHECKER_TIMEOUT_MINUTE, TimeUnit.MINUTES)) {
				result.setRunSuccess(true);
			} else {
				result.setRunSuccess(false);
				result.setErrorMessage("Query consume backlog db timeout, check result is not completely.");
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
