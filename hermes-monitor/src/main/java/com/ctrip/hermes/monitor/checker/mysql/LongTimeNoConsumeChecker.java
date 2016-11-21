package com.ctrip.hermes.monitor.checker.mysql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.admin.core.monitor.event.LongTimeNoConsumeEvent;
import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.admin.core.queue.OffsetMessage;
import com.ctrip.hermes.admin.core.queue.OffsetMessageDao;
import com.ctrip.hermes.admin.core.queue.OffsetMessageEntity;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.DBBasedChecker;
import com.ctrip.hermes.monitor.utils.MonitorUtils;

//@Component(value = "LongTimeNoConsumeChecke")
public class LongTimeNoConsumeChecker extends DBBasedChecker {
	private static final Logger log = LoggerFactory.getLogger(LongTimeNoConsumeChecker.class);

	public static final String ID = "LongTimeNoConsumeChecke";

	private OffsetMessageDao m_offsetDao = PlexusComponentLocator.lookup(OffsetMessageDao.class);

	@Override
	public String name() {
		return ID;
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		CheckerResult result = new CheckerResult();
		if (m_config.isMonitorCheckerEnable()) {
			Meta meta = fetchMeta();
			Map<String, Map<String, Integer>> limits = parseLimits(meta,
			      m_config.getLongTimeNoConsumeCheckerIncludeConsumers(),
			      m_config.getLongTimeNoConsumeCheckerExcludeConsumers());
			for (Entry<String, Map<String, Integer>> topicEntry : limits.entrySet()) {
				Topic topic = meta.findTopic(topicEntry.getKey());
				if (Storage.MYSQL.equals(topic.getStorageType())) {
					Map<String, Integer> consumerLimits = topicEntry.getValue();
					for (Entry<String, Integer> consumerEntry : consumerLimits.entrySet()) {
						ConsumerGroup consumer = topic.findConsumerGroup(consumerEntry.getKey());
						Map<Integer, CreationStamp> stamps = findPartitionConsumeStamps(topic, consumer);
						if (isStampsOutdate(stamps, consumerEntry.getValue())) {
							result.addMonitorEvent(new LongTimeNoConsumeEvent(topic.getName(), consumer.getName(), stamps));
						}
					}
				}
			}
		}
		result.setRunSuccess(true);
		return result;
	}

	private boolean isStampsOutdate(Map<Integer, CreationStamp> stamps, int limit) {
		CreationStamp latest = null;
		for (Entry<Integer, CreationStamp> entry : stamps.entrySet()) {
			latest = MonitorUtils.latestStamp(latest, entry.getValue());
		}
		return latest != null
		      && (System.currentTimeMillis() - latest.getDate().getTime() >= TimeUnit.MINUTES.toMillis(limit));
	}

	public Map<Integer, CreationStamp> findPartitionConsumeStamps(Topic topic, ConsumerGroup consumer) {
		Map<Integer, CreationStamp> partitionConsumeStamps = new HashMap<>();
		for (Partition partition : topic.getPartitions()) {
			CreationStamp latest = MonitorUtils.latestStamp( //
			      findConsumedStamp(topic, partition, 0, consumer), findConsumedStamp(topic, partition, 1, consumer));
			if (latest != null) {
				partitionConsumeStamps.put(partition.getId(), latest);
			}
		}
		return partitionConsumeStamps;
	}

	private CreationStamp findConsumedStamp(Topic topic, Partition partition, int priority, ConsumerGroup consumer) {
		try {
			List<OffsetMessage> off = m_offsetDao.find(topic.getName(), partition.getId(), priority, consumer.getId(),
			      OffsetMessageEntity.READSET_FULL);
			if (!CollectionUtil.isNullOrEmpty(off)) {
				return new CreationStamp(off.get(0).getOffset(), off.get(0).getLastModifiedDate());
			}
		} catch (DalException e) {
			log.debug("Find consumed stamp ({}-{}:{})  failed.", topic.getName(), partition.getId(), consumer.getName(), e);
		}
		return null;
	}

	public Map<String, Map<String, Integer>> parseLimits(Meta meta, String includeStr, String excludeStr) {
		log.info("+++++++++++++++ Long time no consume config string ++++++++++++++++++++");
		log.info("Includes: " + includeStr);
		log.info("Excludes: " + excludeStr);

		Map<String, Map<String, Integer>> includePatterns = //
		JSON.parseObject(includeStr, new TypeReference<Map<String, Map<String, Integer>>>() {
		});

		Map<String, List<String>> excludePatterns = //
		JSON.parseObject(excludeStr, new TypeReference<Map<String, List<String>>>() {
		});
		Map<String, List<String>> topicConsumers = fetchTopicCosnumers(meta);
		Map<String, Map<String, Integer>> includes = parseIncludes(includePatterns, topicConsumers);
		Map<String, List<String>> excludes = parseExcludes(excludePatterns, topicConsumers);
		Map<String, Map<String, Integer>> limits = mergeLimits(includes, excludes);
		return limits;
	}

	@SuppressWarnings("unchecked")
	public Map<String, List<String>> fetchTopicCosnumers(Meta meta) {
		Map<String, List<String>> topicConsumers = new HashMap<>();
		for (Entry<String, Topic> entry : meta.getTopics().entrySet()) {
			topicConsumers.put(entry.getKey(),
			      (List<String>) CollectionUtils.collect(entry.getValue().getConsumerGroups(), new Transformer() {
				      @Override
				      public Object transform(Object input) {
					      return ((ConsumerGroup) input).getName();
				      }
			      }));
		}
		return topicConsumers;
	}

	private Map<Pair<String, String>, Boolean> m_matchMap = new HashMap<>();

	private boolean matches(String pattern, String input) {
		Pair<String, String> p = new Pair<String, String>(pattern, input);
		if (!m_matchMap.containsKey(p)) {
			m_matchMap.put(p, Pattern.matches(pattern, input));
		}
		return m_matchMap.get(p);
	}

	private String matches(Collection<String> patterns, String input) {
		String matched = null;
		for (String pattern : patterns) {
			if (matches(pattern, input) && (matched == null || pattern.length() > matched.length())) {
				matched = pattern;
			}
		}
		return matched;
	}

	private List<String> matches(Collection<String> patterns, Collection<String> inputs) {
		List<String> matched = new ArrayList<String>();
		for (String input : inputs) {
			if (matches(patterns, input) != null) {
				matched.add(input);
			}
		}
		return matched;
	}

	private <T> Map<String, T> matches(Map<String, T> patterns, Collection<String> inputs) {
		Map<String, T> matched = new HashMap<>();
		for (String input : inputs) {
			String matchedPatternKey = matches(patterns.keySet(), input);
			if (matchedPatternKey != null) {
				matched.put(input, patterns.get(matchedPatternKey));
			}
		}
		return matched;
	}

	private Map<String, List<String>> parseExcludes(Map<String, List<String>> excludePatterns,
	      Map<String, List<String>> topicConsumers) {
		Map<String, List<String>> excludes = new HashMap<String, List<String>>();
		Set<String> topicPatterns = excludePatterns.keySet();
		for (Entry<String, List<String>> entry : topicConsumers.entrySet()) {
			String topic = entry.getKey();
			List<String> consumers = entry.getValue();
			String matchedTopicPattern = matches(topicPatterns, topic);
			if (matchedTopicPattern != null) {
				List<String> consumerPatterns = excludePatterns.get(matchedTopicPattern);
				List<String> matchedConsumers = matches(consumerPatterns, consumers);
				if (matchedConsumers.size() > 0) {
					excludes.put(topic, matchedConsumers);
				}
			}
		}
		return excludes;
	}

	// topic, consumer, limit
	private Map<String, Map<String, Integer>> parseIncludes(Map<String, Map<String, Integer>> includePatterns,
	      Map<String, List<String>> topicConsumers) {
		Map<String, Map<String, Integer>> includes = new HashMap<>();
		Set<String> topicPatterns = includePatterns.keySet();
		for (Entry<String, List<String>> entry : topicConsumers.entrySet()) {
			String topic = entry.getKey();
			List<String> consumers = entry.getValue();
			String matchedTopicPattern = matches(topicPatterns, topic);
			if (matchedTopicPattern != null) {
				Map<String, Integer> matchedConsumers = matches(includePatterns.get(matchedTopicPattern), consumers);
				if (matchedConsumers.size() > 0) {
					includes.put(topic, matchedConsumers);
				}
			}
		}
		return includes;
	}

	private Map<String, Map<String, Integer>> mergeLimits(Map<String, Map<String, Integer>> includes,
	      Map<String, List<String>> excludes) {
		for (Entry<String, List<String>> entry : excludes.entrySet()) {
			String topic = entry.getKey();
			Map<String, Integer> consumerLimits = includes.get(topic);
			if (consumerLimits != null) {
				for (String consumer : entry.getValue()) {
					consumerLimits.remove(consumer);
				}
				if (consumerLimits.size() == 0) {
					includes.remove(topic);
				}
			}
		}
		return includes;
	}
}
